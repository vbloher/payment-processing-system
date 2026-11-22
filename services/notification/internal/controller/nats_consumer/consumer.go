package nats_consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	// settleShare: one AckWait/settleShare of each delivery is kept back from the handler to
	// acknowledge or dead-letter the message before the broker redelivers it.
	settleShare = 5

	// dlqAttempts is how many times a dead letter is published before giving up.
	dlqAttempts = 3
)

var ErrInvalidConfig = errors.New(
	"nats consumer: durable must be set, max deliver and ack wait must be positive, retry delay must not be negative")

type Config struct {
	Durable    string        `env:"NATS_CONSUMER_DURABLE"     envDefault:"notification"`
	MaxDeliver int           `env:"NATS_CONSUMER_MAX_DELIVER" envDefault:"5"`
	RetryDelay time.Duration `env:"NATS_CONSUMER_RETRY_DELAY" envDefault:"1s"`
	AckWait    time.Duration `env:"NATS_CONSUMER_ACK_WAIT"    envDefault:"30s"`
}

// HandlerTimeout is how long the handler may take with one delivery: four fifths of AckWait.
// The rest is left to settle the message, a dead letter included, before AckWait runs out.
func (c Config) HandlerTimeout() time.Duration {
	return c.AckWait - c.settleTimeout()
}

func (c Config) settleTimeout() time.Duration {
	return c.AckWait / settleShare
}

// Consumer feeds the EVENTS stream to the use case through a durable pull consumer, one message
// at a time. A handled message is acknowledged; a failed one is redelivered after RetryDelay; a
// message that cannot be decoded, or whose last delivery fails, goes to the dead-letter stream
// and is terminated.
type Consumer struct {
	config     Config
	maxDeliver uint64
	js         jetstream.JetStream
	usecase    *usecase.UseCase
	consume    jetstream.ConsumeContext
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

	// The broker counts a delivery, and starts its AckWait, when it hands the message to the
	// client. Messages are handled one at a time, so a prefetched message would spend its
	// deliveries waiting in the buffer: pull only the message about to be handled.
	consumer.consume, err = durable.Consume(consumer.receive,
		jetstream.PullMaxMessages(1), jetstream.ConsumeErrHandler(logConsumeError))
	if err != nil {
		return nil, fmt.Errorf("durable.Consume: %w", err)
	}

	log.Info().Msg("nats consumer: started, durable: " + c.Durable)

	return consumer, nil
}

// receive is called on the subscription's goroutine, one message at a time.
func (c *Consumer) receive(msg jetstream.Msg) {
	// The handler's context is its own: stopping the service must not fail a message in flight,
	// which on its last delivery would send it to the DLQ. It ends before AckWait, so that the
	// message can still be settled.
	ctx, cancel := context.WithTimeout(context.Background(), c.config.HandlerTimeout())
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

// deadLetter publishes a copy of the message to the DLQ, then terminates the original.
func (c *Consumer) deadLetter(ctx context.Context, msg jetstream.Msg) {
	// The handler may have failed by running out of time, so ctx can be spent. The publish gets
	// time of its own, and the broker's AckWait restarts so that it does not redeliver meanwhile.
	logSettleError("in progress", msg.InProgress())

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

	err = c.publishDeadLetter(context.WithoutCancel(ctx), dead, opts)
	if err != nil {
		// Residual risk: the message is left unacknowledged. While deliveries remain, the broker
		// redelivers it after AckWait and dead-lettering is tried again. After the last one it
		// stays in the EVENTS stream, delivered no more, and this log is the only trace of it.
		log.Error().Err(err).Str("subject", msg.Subject()).
			Msg("nats consumer: dead-letter publish failed, the message stays unacknowledged in the stream")

		return
	}

	logSettleError("term", msg.Term())
}

// publishDeadLetter makes dlqAttempts attempts, each within an equal share of the settle time.
// An attempt that fails early waits out its share, so the attempts are spread over that time.
func (c *Consumer) publishDeadLetter(ctx context.Context, dead *nats.Msg, opts []jetstream.PublishOpt) error {
	share := c.config.settleTimeout() / dlqAttempts

	var err error

	for attempt := 1; attempt <= dlqAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, share)

		_, err = c.js.PublishMsg(attemptCtx, dead, opts...)
		if err == nil {
			cancel()

			return nil
		}

		log.Warn().Err(err).Int("attempt", attempt).Msg("nats consumer: dead-letter publish")

		if attempt < dlqAttempts {
			<-attemptCtx.Done()
		}

		cancel()
	}

	return fmt.Errorf("js.PublishMsg %s: %w", dlqSubject, err)
}

// Close stops fetching and returns once the subscription has drained: the message in flight and
// the one already fetched, if any, are handled first, so no fetched delivery is thrown away.
// In drain mode the subscription closes only after its last callback has returned.
func (c *Consumer) Close() {
	log.Info().Msg("nats consumer: closing")

	c.consume.Drain()
	<-c.consume.Closed()

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
