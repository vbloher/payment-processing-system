package nats_consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/notification/internal/usecase"
)

const (
	filterSubject = "events.>"
	dlqSubject    = events.DLQSubjectPrefix + "notification"

	// HeaderOriginalSubject is set on a dead letter to the subject it was consumed from.
	HeaderOriginalSubject = "Dlq-Original-Subject"
)

var ErrInvalidConfig = errors.New(
	"nats consumer: durable must be set, max deliver and ack wait must be positive, retry delay must not be negative")

type Config struct {
	Durable    string        `env:"NATS_CONSUMER_DURABLE"     envDefault:"notification"`
	MaxDeliver int           `env:"NATS_CONSUMER_MAX_DELIVER" envDefault:"5"`
	RetryDelay time.Duration `env:"NATS_CONSUMER_RETRY_DELAY" envDefault:"1s"`
	AckWait    time.Duration `env:"NATS_CONSUMER_ACK_WAIT"    envDefault:"30s"`
}

// Consumer feeds the EVENTS stream to the use case through a durable pull consumer. A handled
// message is acknowledged; a failed one is redelivered after RetryDelay; a message that cannot be
// decoded, or whose last delivery fails, goes to the dead-letter stream and is terminated.
type Consumer struct {
	config     Config
	maxDeliver uint64
	js         jetstream.JetStream
	usecase    *usecase.UseCase
	consume    jetstream.ConsumeContext

	mu       sync.Mutex
	closing  bool
	inflight sync.WaitGroup
}

func New(ctx context.Context, c Config, client *natsjs.Client, uc *usecase.UseCase) (*Consumer, error) {
	if c.Durable == "" || c.MaxDeliver < 1 || c.AckWait <= 0 || c.RetryDelay < 0 {
		return nil, fmt.Errorf("%w: %+v", ErrInvalidConfig, c)
	}

	durable, err := client.JS.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable:       c.Durable,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       c.AckWait,
		MaxDeliver:    c.MaxDeliver,
	})
	if err != nil {
		return nil, fmt.Errorf("js.CreateOrUpdateConsumer: %w", err)
	}

	consumer := &Consumer{
		config:     c,
		maxDeliver: uint64(c.MaxDeliver),
		js:         client.JS,
		usecase:    uc,
	}

	consumer.consume, err = durable.Consume(consumer.receive, jetstream.ConsumeErrHandler(logConsumeError))
	if err != nil {
		return nil, fmt.Errorf("durable.Consume: %w", err)
	}

	log.Info().Msg("nats consumer: started, durable: " + c.Durable)

	return consumer, nil
}

// receive is called on the subscription's goroutine, one message at a time.
func (c *Consumer) receive(msg jetstream.Msg) {
	c.mu.Lock()

	if c.closing {
		c.mu.Unlock()
		// Handed back at once, so that another instance can take it.
		logSettleError("nak", msg.Nak())

		return
	}

	c.inflight.Add(1)
	c.mu.Unlock()

	defer c.inflight.Done()

	// The handler's context is its own: stopping the service must not fail a message in flight,
	// which on its last delivery would send it to the DLQ. Past AckWait the broker redelivers the
	// message anyway, so the handler gets no longer than that.
	ctx, cancel := context.WithTimeout(context.Background(), c.config.AckWait)
	defer cancel()

	c.handle(ctx, msg)
}

func (c *Consumer) handle(ctx context.Context, msg jetstream.Msg) {
	subject := msg.Subject()

	ctx, span := tracer.Start(tracer.Extract(ctx, carrier(msg.Headers())), "nats consume "+subject,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			semconv.MessagingDestinationName(subject),
			semconv.MessagingConsumerGroupName(c.config.Durable),
		),
	)
	defer span.End()

	e, err := events.Unmarshal(msg.Data())
	if err != nil {
		recordError(span, err)
		log.Error().Err(err).Str("subject", subject).Msg("nats consumer: undecodable message, dead-lettering it")
		c.deadLetter(ctx, msg)

		return
	}

	span.SetAttributes(semconv.MessagingMessageID(e.ID.String()))

	err = c.usecase.HandleEvent(ctx, e)
	if err == nil {
		logSettleError("ack", msg.Ack())

		return
	}

	recordError(span, err)

	if c.lastDelivery(msg) {
		log.Error().Err(err).Str("event_id", e.ID.String()).Str("subject", subject).
			Msg("nats consumer: last delivery failed, dead-lettering the event")
		c.deadLetter(ctx, msg)

		return
	}

	log.Warn().Err(err).Str("event_id", e.ID.String()).Str("subject", subject).
		Msg("nats consumer: handling failed, the event will be redelivered")
	logSettleError("nak", msg.NakWithDelay(c.config.RetryDelay))
}

// lastDelivery reports whether the broker will not deliver the message again. Without metadata
// it answers false: the message is retried and the broker still stops at MaxDeliver.
func (c *Consumer) lastDelivery(msg jetstream.Msg) bool {
	md, err := msg.Metadata()
	if err != nil {
		log.Error().Err(err).Msg("nats consumer: msg.Metadata")

		return false
	}

	return md.NumDelivered >= c.maxDeliver
}

// deadLetter publishes a copy of the message to the DLQ, then terminates the original. When the
// copy cannot be published the original is not terminated: it is retried while deliveries remain,
// and after the last one it stays in the EVENTS stream.
func (c *Consumer) deadLetter(ctx context.Context, msg jetstream.Msg) {
	dead := &nats.Msg{Subject: dlqSubject, Data: msg.Data(), Header: nats.Header{}}

	for k, v := range msg.Headers() {
		if !strings.HasPrefix(k, "Nats-") {
			dead.Header[k] = v
		}
	}

	dead.Header.Set(HeaderOriginalSubject, msg.Subject())

	var opts []jetstream.PublishOpt

	// If the Term below is lost, the redelivered message is dead-lettered again under the same
	// ID, which the DLQ stream drops as a duplicate.
	md, err := msg.Metadata()
	if err == nil {
		opts = append(opts, jetstream.WithMsgID(fmt.Sprintf("%s.%s.%d", c.config.Durable, md.Stream, md.Sequence.Stream)))
	}

	_, err = c.js.PublishMsg(ctx, dead, opts...)
	if err != nil {
		log.Error().Err(err).Str("subject", msg.Subject()).Msg("nats consumer: dead-letter publish failed")
		logSettleError("nak", msg.NakWithDelay(c.config.RetryDelay))

		return
	}

	logSettleError("term", msg.Term())
}

// Close stops fetching and waits for the message in flight. A message that reaches the handler
// after that is handed back with a Nak; messages still buffered are dropped unacknowledged and
// redelivered after AckWait.
func (c *Consumer) Close() {
	log.Info().Msg("nats consumer: closing")

	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()

	c.consume.Stop()
	c.inflight.Wait()

	log.Info().Msg("nats consumer: closed")
}

// carrier returns the message headers as a trace propagation carrier.
func carrier(h nats.Header) map[string]string {
	m := make(map[string]string, len(h))

	for k, v := range h {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}

	return m
}

func recordError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func logSettleError(action string, err error) {
	if err != nil {
		log.Error().Err(err).Msg("nats consumer: " + action)
	}
}

func logConsumeError(_ jetstream.ConsumeContext, err error) {
	log.Warn().Err(err).Msg("nats consumer: consume")
}
