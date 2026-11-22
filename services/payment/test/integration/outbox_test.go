//go:build integration

package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/metrics"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
	"github.com/vbloher/payment-processing-system/pkg/transaction"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/nats_publisher"
	adapter "github.com/vbloher/payment-processing-system/services/payment/internal/adapter/postgres"
	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/outbox_worker"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

func Test_Outbox(t *testing.T) {
	suite.Run(t, &OutboxSuite{})
}

// OutboxSuite owns its Postgres and NATS: the payment suite deletes rows in the same tables.
type OutboxSuite struct {
	suite.Suite
	*require.Assertions

	pool      *pgxpool.Pool
	tx        *transaction.Manager
	pg        *adapter.Postgres
	nats      *natsjs.Client
	publisher *nats_publisher.Publisher
}

func (s *OutboxSuite) SetupSuite() {
	s.Assertions = s.Require()

	otel.SilentModeInit()

	container := testinfra.StartPostgres(s.T(), "payment")

	migrations, err := filepath.Abs("../../migration/postgres")
	s.NoError(err)

	testinfra.Migrate(s.T(), container.URL("payment"), migrations)

	pool, err := postgres.New(context.Background(), container.Config("payment"))
	s.NoError(err)

	s.T().Cleanup(pool.Close)

	s.pool = pool.Pool
	s.tx = transaction.New(pool)
	s.pg = adapter.New(s.tx)

	nats, err := natsjs.New(context.Background(), testinfra.StartNATS(s.T()))
	s.NoError(err)

	s.T().Cleanup(nats.Close)

	s.NoError(nats.EnsureStreams(context.Background(), events.StreamConfig(), events.DLQStreamConfig()))

	s.nats = nats
	s.publisher = nats_publisher.New(nats, metrics.NewProcess(prometheus.NewRegistry()))
}

func (s *OutboxSuite) SetupTest() {
	s.Assertions = s.Require()

	_, err := s.pool.Exec(s.T().Context(), "DELETE FROM outbox")
	s.NoError(err)
}

func (s *OutboxSuite) newUseCase(p usecase.Publisher) *usecase.UseCase {
	return usecase.New(s.tx, s.pg, nil, nil, p, nil)
}

func (s *OutboxSuite) event(subject string) domain.Event {
	return domain.Event{ID: uuid.New(), Subject: subject, Payload: []byte(`{"n":"` + uuid.NewString() + `"}`), Headers: map[string]string{}}
}

func (s *OutboxSuite) save(evs ...domain.Event) {
	s.NoError(s.tx.Wrap(s.T().Context(), func(ctx context.Context) error {
		return s.pg.SaveOutbox(ctx, evs...)
	}))
}

func (s *OutboxSuite) outboxCount() int {
	var n int

	s.NoError(s.pool.QueryRow(s.T().Context(), "SELECT count(*) FROM outbox").Scan(&n))

	return n
}

func (s *OutboxSuite) lastSeq() uint64 {
	stream, err := s.nats.JS.Stream(s.T().Context(), events.StreamName)
	s.NoError(err)

	info, err := stream.Info(s.T().Context())
	s.NoError(err)

	return info.State.LastSeq
}

// messagesAfter returns the stream messages with a sequence above seq.
func (s *OutboxSuite) messagesAfter(seq uint64) []*streamMsg {
	stream, err := s.nats.JS.Stream(s.T().Context(), events.StreamName)
	s.NoError(err)

	var result []*streamMsg

	for n := seq + 1; n <= s.lastSeq(); n++ {
		m, err := stream.GetMsg(s.T().Context(), n)
		s.NoError(err)

		result = append(result, &streamMsg{Subject: m.Subject, Data: m.Data, MsgID: m.Header.Get("Nats-Msg-Id")})
	}

	return result
}

type streamMsg struct {
	Subject string
	Data    []byte
	MsgID   string
}

func (s *OutboxSuite) Test_SendOutbox_PublishesAndDeletes() {
	before := s.lastSeq()
	a, b := s.event("events.payment.succeeded"), s.event("events.payment.failed")
	s.save(a, b)

	sent, err := s.newUseCase(s.publisher).SendOutbox(s.T().Context(), 10)
	s.NoError(err)
	s.Equal(2, sent)
	s.Zero(s.outboxCount())

	msgs := s.messagesAfter(before)
	s.Len(msgs, 2)

	got := map[string]*streamMsg{}
	for _, m := range msgs {
		got[m.MsgID] = m
	}

	s.Equal(a.Payload, got[a.ID.String()].Data)
	s.Equal(a.Subject, got[a.ID.String()].Subject)
	s.Equal(b.Payload, got[b.ID.String()].Data)
	s.Equal(b.Subject, got[b.ID.String()].Subject)
}

// Spec scenario 12: NATS is down, the events stay in the outbox and go out on the next attempt.
func (s *OutboxSuite) Test_SendOutbox_PublishFailureKeepsEvents() {
	ev := s.event("events.payment.succeeded")
	s.save(ev)

	errNATS := errors.New("nats is down")

	_, err := s.newUseCase(failingPublisher{err: errNATS}).SendOutbox(s.T().Context(), 10)
	s.ErrorIs(err, errNATS)
	s.Equal(1, s.outboxCount())

	before := s.lastSeq()

	sent, err := s.newUseCase(s.publisher).SendOutbox(s.T().Context(), 10)
	s.NoError(err)
	s.Equal(1, sent)
	s.Zero(s.outboxCount())
	s.Equal(before+1, s.lastSeq())
}

type failingPublisher struct{ err error }

func (f failingPublisher) Publish(context.Context, ...domain.Event) error { return f.err }

func (s *OutboxSuite) Test_Publish_SameEventIDIsDeduplicated() {
	before := s.lastSeq()
	ev := s.event("events.payment.succeeded")

	s.NoError(s.publisher.Publish(s.T().Context(), ev))
	s.NoError(s.publisher.Publish(s.T().Context(), ev))

	s.Equal(before+1, s.lastSeq())
}

func (s *OutboxSuite) Test_OutboxWorker_DrainsInBackground() {
	worker, err := outbox_worker.New(outbox_worker.Config{Interval: 50 * time.Millisecond, Limit: 100}, s.newUseCase(s.publisher))
	s.NoError(err)

	s.T().Cleanup(worker.Close)

	s.save(s.event("events.payment.succeeded"))

	s.Eventually(func() bool { return s.outboxCount() == 0 }, 5*time.Second, 20*time.Millisecond)
}
