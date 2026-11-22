//go:build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/services/notification"
	"github.com/vbloher/payment-processing-system/services/notification/config"
	"github.com/vbloher/payment-processing-system/services/notification/internal/adapter/webhook"
	"github.com/vbloher/payment-processing-system/services/notification/internal/controller/nats_consumer"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

const (
	durable    = "notification"
	maxDeliver = 3
	dlqSubject = events.DLQSubjectPrefix + "notification"

	waitTimeout = 10 * time.Second
	waitTick    = 20 * time.Millisecond
	stopTimeout = 30 * time.Second
)

func Test_Consumer(t *testing.T) {
	suite.Run(t, &ConsumerSuite{})
}

// ConsumerSuite runs the notification service in-process through notification.Run, with its own
// Postgres and NATS and an httptest.Server as the webhook. Tests share the service, so each one
// asserts only on its own event IDs and on stream messages published after it started.
type ConsumerSuite struct {
	suite.Suite
	*require.Assertions

	pool    *pgxpool.Pool
	nats    *natsjs.Client
	webhook *webhookServer

	config      config.Config // the service's configuration for every test but the one that restarts it
	stopService context.CancelFunc
	serviceDone chan error
}

func (s *ConsumerSuite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	pg := testinfra.StartPostgres(s.T(), "notification")

	migrations, err := filepath.Abs("../../migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), pg.URL("notification"), migrations)

	natsConfig := testinfra.StartNATS(s.T())

	// Cleanups run in reverse: the service stops before the webhook server and the containers.
	s.webhook = newWebhookServer()
	s.T().Cleanup(s.webhook.Close)

	s.config = config.Config{Postgres: pg.Config("notification"), NATS: natsConfig}
	s.config.Consumer = nats_consumer.Config{
		Durable: durable, MaxDeliver: maxDeliver, RetryDelay: 100 * time.Millisecond, AckWait: 30 * time.Second,
	}
	s.config.Webhook = webhook.Config{URL: s.webhook.URL, Timeout: 2 * time.Second}

	s.T().Cleanup(s.stop)
	s.start(s.config)

	pool, err := postgres.New(context.Background(), pg.Config("notification"))
	s.NoError(err)
	s.T().Cleanup(pool.Close)

	s.pool = pool.Pool

	s.nats, err = natsjs.New(context.Background(), natsConfig)
	s.NoError(err)
	s.T().Cleanup(s.nats.Close)
}

func (s *ConsumerSuite) SetupTest() {
	s.Assertions = s.Require()
}

// start runs the service with the configuration on a free port and waits until it is ready.
func (s *ConsumerSuite) start(c config.Config) {
	c.HTTP.Port = testinfra.FreePort(s.T())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- notification.Run(ctx, c) }()

	s.stopService, s.serviceDone = cancel, done

	testinfra.WaitHTTP(s.T(), "http://127.0.0.1:"+c.HTTP.Port+"/ready")
}

// stop cancels the service and requires its Run to return nil.
func (s *ConsumerSuite) stop() {
	if s.stopService == nil {
		return
	}

	s.stopService()
	s.stopService = nil

	select {
	case err := <-s.serviceDone:
		s.Assert().NoError(err, "notification.Run")
	case <-time.After(stopTimeout):
		s.Assert().Fail("notification.Run did not return after its context was cancelled")
	}
}

// restart stops the service and starts it again with the configuration, on the same durable.
func (s *ConsumerSuite) restart(c config.Config) {
	s.stop()
	s.start(c)
}

func (s *ConsumerSuite) Test_Consumer_DeliversOnce() {
	e := s.envelope()
	s.publishEnvelope(e, e.ID.String())

	s.waitNotificationStatus(e.ID, domain.StatusSent)

	calls := s.webhook.callsFor(e.ID)
	s.Len(calls, 1)
	s.Equal(http.MethodPost, calls[0].method)
	s.Equal(e.ID.String(), calls[0].idempotencyKey)
	s.Equal("application/json", calls[0].contentType)

	got, err := events.Unmarshal(calls[0].body)
	s.NoError(err)
	s.Equal(e.ID, got.ID)
	s.Equal(e.Type, got.Type)
	s.Equal(e.PaymentID, got.PaymentID)
	s.True(e.OccurredAt.Equal(got.OccurredAt), "occurred_at: want %s, got %s", e.OccurredAt, got.OccurredAt)
	s.JSONEq(string(e.Data), string(got.Data))

	s.Equal(1, s.countRows("notification", e.ID))
}

// Spec scenario 11: the broker delivers the same event twice; the inbox makes it one effect.
func (s *ConsumerSuite) Test_Consumer_RedeliveryHasOneEffect() {
	e := s.envelope()

	payload, err := e.Marshal()
	s.NoError(err)

	before := s.dlqLastSeq()

	// Different message IDs, so that JetStream stores both copies instead of deduplicating them.
	s.publish(e.Type.Subject(), payload, uuid.NewString())
	second := s.publish(e.Type.Subject(), payload, uuid.NewString())

	s.waitAcked(second)

	s.Equal(1, s.countRows("inbox", e.ID))
	s.Equal(1, s.countRows("notification", e.ID))
	s.Len(s.webhook.callsFor(e.ID), 1)
	s.Equal(domain.StatusSent, s.notificationStatus(e.ID))
	// The copy was handled as a duplicate, not rejected until it reached the DLQ.
	s.Empty(s.deadLettersAfter(before, payload))
}

func (s *ConsumerSuite) Test_Consumer_RetriesFailedDelivery() {
	e := s.envelope()
	s.webhook.fail(e.ID, 2)

	s.publishEnvelope(e, e.ID.String())

	s.waitNotificationStatus(e.ID, domain.StatusSent)

	calls := s.webhook.callsFor(e.ID)
	s.Len(calls, 3)

	for _, call := range calls {
		s.Equal(e.ID.String(), call.idempotencyKey)
	}

	s.Equal(1, s.countRows("notification", e.ID))
}

func (s *ConsumerSuite) Test_Consumer_PoisonGoesToDLQ() {
	poison := []byte("not json")
	before := s.dlqLastSeq()

	s.publish(events.TypePaymentSucceeded.Subject(), poison, uuid.NewString())

	s.Eventually(func() bool { return len(s.deadLettersAfter(before, poison)) > 0 }, 5*time.Second, waitTick)

	// A valid envelope behind the poison message is still delivered.
	e := s.envelope()
	seq := s.publishEnvelope(e, e.ID.String())

	s.waitNotificationStatus(e.ID, domain.StatusSent)
	s.Len(s.webhook.callsFor(e.ID), 1)

	// Every message up to the valid one is settled, so the poison message reached the DLQ once.
	s.waitAcked(seq)

	dead := s.deadLettersAfter(before, poison)
	s.Len(dead, 1)
	s.Equal(events.TypePaymentSucceeded.Subject(), dead[0].Header.Get(nats_consumer.HeaderOriginalSubject))
}

func (s *ConsumerSuite) Test_Consumer_ExhaustedDeliveryGoesToDLQ() {
	e := s.envelope()
	s.webhook.fail(e.ID, failAlways)

	payload, err := e.Marshal()
	s.NoError(err)

	before := s.dlqLastSeq()
	seq := s.publish(e.Type.Subject(), payload, e.ID.String())

	s.Eventually(func() bool { return len(s.deadLettersAfter(before, payload)) > 0 }, waitTimeout, waitTick)

	// The broker stops at MaxDeliver and the message is terminated after it reached the DLQ.
	s.waitAcked(seq)
	s.Len(s.deadLettersAfter(before, payload), 1)
	s.Len(s.webhook.callsFor(e.ID), maxDeliver)
	s.Equal(domain.StatusPending, s.notificationStatus(e.ID))
}

// A delivery that runs into the handler deadline is still dead-lettered on its last attempt,
// although the handler's context has expired by then. The consumer pulls one message at a time,
// so a message queued behind the stuck one spends no deliveries while it waits.
func (s *ConsumerSuite) Test_Consumer_TimedOutDeliveryGoesToDLQ() {
	// Only this test runs with a short AckWait: the handler deadline is 1.6s.
	c := s.config
	c.Consumer.AckWait = 2 * time.Second
	c.Webhook.Timeout = time.Second

	s.restart(c)
	defer s.restart(s.config)

	stuck, next := s.envelope(), s.envelope()

	payload, err := stuck.Marshal()
	s.NoError(err)

	s.waitAcked(s.streamLastSeq())

	// An open transaction holding stuck's inbox row makes the consumer's insert wait, every
	// delivery, until the handler deadline cancels it.
	lock, err := s.pool.Begin(s.T().Context())
	s.NoError(err)

	defer func() { _ = lock.Rollback(context.Background()) }()

	_, err = lock.Exec(s.T().Context(),
		"INSERT INTO inbox (event_id, event_type) VALUES ($1, $2)", stuck.ID, string(stuck.Type))
	s.NoError(err)

	before := s.dlqLastSeq()
	stuckSeq := s.publish(stuck.Type.Subject(), payload, stuck.ID.String())

	s.Eventually(func() bool { return s.lastDelivered() >= stuckSeq }, waitTimeout, waitTick)

	// While stuck is being handled, next stays on the server: nothing was fetched ahead.
	nextSeq := s.publishEnvelope(next, next.ID.String())
	s.Never(func() bool { return s.lastDelivered() >= nextSeq }, 300*time.Millisecond, waitTick,
		"message %d was fetched while message %d was being handled", nextSeq, stuckSeq)

	// next goes through between stuck's attempts; stuck reaches the DLQ after its last one.
	s.waitNotificationStatus(next.ID, domain.StatusSent)
	s.Eventually(func() bool { return len(s.deadLettersAfter(before, payload)) > 0 },
		3*waitTimeout, waitTick, "event %s did not reach the DLQ", stuck.ID)

	s.waitAcked(nextSeq)
	s.Len(s.deadLettersAfter(before, payload), 1)
	s.Empty(s.webhook.callsFor(stuck.ID))
	s.Len(s.webhook.callsFor(next.ID), 1)
}

func Test_Sender_NoURL(t *testing.T) {
	otel.SilentModeInit()

	e, err := events.New(events.TypePaymentSucceeded, uuid.New(), events.PaymentData{Amount: 100, Currency: "EUR"})
	require.NoError(t, err)

	n, err := domain.NewNotification(e)
	require.NoError(t, err)

	require.NoError(t, webhook.New(webhook.Config{}).Send(t.Context(), n))
}

func (s *ConsumerSuite) envelope() events.Envelope {
	e, err := events.New(events.TypePaymentSucceeded, uuid.New(), events.PaymentData{
		AccountID: uuid.New(), Amount: 3000, Currency: "EUR", Status: "succeeded",
	})
	s.NoError(err)

	return e
}

// publishEnvelope publishes the envelope on its subject and returns its stream sequence.
func (s *ConsumerSuite) publishEnvelope(e events.Envelope, msgID string) uint64 {
	payload, err := e.Marshal()
	s.NoError(err)

	return s.publish(e.Type.Subject(), payload, msgID)
}

// publish sends data to JetStream with the message ID and returns its stream sequence.
func (s *ConsumerSuite) publish(subject string, data []byte, msgID string) uint64 {
	ack, err := s.nats.JS.PublishMsg(s.T().Context(), &nats.Msg{Subject: subject, Data: data}, jetstream.WithMsgID(msgID))
	s.NoError(err)
	s.False(ack.Duplicate, "message %s was deduplicated", msgID)

	return ack.Sequence
}

// waitAcked waits until the consumer has acknowledged or terminated every message up to seq.
func (s *ConsumerSuite) waitAcked(seq uint64) {
	s.Eventually(func() bool {
		consumer, err := s.nats.JS.Consumer(context.Background(), events.StreamName, durable)
		if err != nil {
			return false
		}

		info, err := consumer.Info(context.Background())

		return err == nil && info.AckFloor.Stream >= seq
	}, waitTimeout, waitTick, "messages up to stream sequence %d are not settled", seq)
}

// lastDelivered returns the stream sequence of the last message delivered to the consumer.
// It asserts nothing, so that Eventually can call it.
func (s *ConsumerSuite) lastDelivered() uint64 {
	consumer, err := s.nats.JS.Consumer(context.Background(), events.StreamName, durable)
	if err != nil {
		return 0
	}

	info, err := consumer.Info(context.Background())
	if err != nil {
		return 0
	}

	return info.Delivered.Stream
}

func (s *ConsumerSuite) streamLastSeq() uint64 {
	stream, err := s.nats.JS.Stream(s.T().Context(), events.StreamName)
	s.NoError(err)

	info, err := stream.Info(s.T().Context())
	s.NoError(err)

	return info.State.LastSeq
}

func (s *ConsumerSuite) dlqLastSeq() uint64 {
	stream, err := s.nats.JS.Stream(s.T().Context(), events.DLQStreamName)
	s.NoError(err)

	info, err := stream.Info(s.T().Context())
	s.NoError(err)

	return info.State.LastSeq
}

// deadLettersAfter returns the DLQ messages above seq on this service's DLQ subject whose body
// is data. It asserts nothing, so that Eventually can call it.
func (s *ConsumerSuite) deadLettersAfter(seq uint64, data []byte) []*jetstream.RawStreamMsg {
	ctx := context.Background()

	stream, err := s.nats.JS.Stream(ctx, events.DLQStreamName)
	if err != nil {
		return nil
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil
	}

	var found []*jetstream.RawStreamMsg

	for n := seq + 1; n <= info.State.LastSeq; n++ {
		m, err := stream.GetMsg(ctx, n)
		if err == nil && m.Subject == dlqSubject && bytes.Equal(m.Data, data) {
			found = append(found, m)
		}
	}

	return found
}

func (s *ConsumerSuite) countRows(table string, eventID uuid.UUID) int {
	var n int

	s.NoError(s.pool.QueryRow(s.T().Context(),
		"SELECT count(*) FROM "+table+" WHERE event_id = $1", eventID).Scan(&n))

	return n
}

func (s *ConsumerSuite) notificationStatus(eventID uuid.UUID) domain.Status {
	var status string

	s.NoError(s.pool.QueryRow(s.T().Context(),
		"SELECT status FROM notification WHERE event_id = $1", eventID).Scan(&status))

	return domain.Status(status)
}

func (s *ConsumerSuite) waitNotificationStatus(eventID uuid.UUID, want domain.Status) {
	s.Eventually(func() bool {
		var status string

		err := s.pool.QueryRow(context.Background(),
			"SELECT status FROM notification WHERE event_id = $1", eventID).Scan(&status)

		return err == nil && domain.Status(status) == want
	}, waitTimeout, waitTick, "notification of event %s is not %s", eventID, want)
}

// failAlways makes the webhook answer 500 to every call for the key.
const failAlways = -1

// webhookServer is the webhook receiver. It records every call and answers 200, except for the
// keys it was told to fail.
type webhookServer struct {
	*httptest.Server

	mu    sync.Mutex
	calls map[string][]webhookCall
	fails map[string]int // number of 500 answers left per Idempotency-Key, or failAlways
}

type webhookCall struct {
	method         string
	idempotencyKey string
	contentType    string
	body           []byte
}

func newWebhookServer() *webhookServer {
	w := &webhookServer{calls: map[string][]webhookCall{}, fails: map[string]int{}}
	w.Server = httptest.NewServer(http.HandlerFunc(w.serve))

	return w
}

func (w *webhookServer) serve(rw http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)

		return
	}

	key := r.Header.Get("Idempotency-Key")

	w.mu.Lock()
	w.calls[key] = append(w.calls[key], webhookCall{
		method: r.Method, idempotencyKey: key, contentType: r.Header.Get("Content-Type"), body: body,
	})

	status := http.StatusOK

	switch left := w.fails[key]; {
	case left == failAlways:
		status = http.StatusInternalServerError
	case left > 0:
		w.fails[key] = left - 1
		status = http.StatusInternalServerError
	}
	w.mu.Unlock()

	rw.WriteHeader(status)
}

// fail makes the next n calls for the event answer 500; n may be failAlways.
func (w *webhookServer) fail(eventID uuid.UUID, n int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.fails[eventID.String()] = n
}

func (w *webhookServer) callsFor(eventID uuid.UUID) []webhookCall {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]webhookCall(nil), w.calls[eventID.String()]...)
}
