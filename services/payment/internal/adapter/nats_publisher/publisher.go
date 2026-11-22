package nats_publisher

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/vbloher/payment-processing-system/pkg/metrics"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

type Publisher struct {
	js      jetstream.JetStream
	metrics *metrics.Process
}

func New(c *natsjs.Client, m *metrics.Process) *Publisher {
	return &Publisher{js: c.JS, metrics: m}
}

// Publish sends each event with its ID as the JetStream message ID, so a retry after a partial
// failure does not duplicate the events that already went out.
func (p *Publisher) Publish(ctx context.Context, events ...domain.Event) error {
	const publish = "publish"

	defer p.metrics.Duration(publish, time.Now())

	for _, e := range events {
		err := p.publishOne(ctx, e)
		if err != nil {
			p.metrics.Total(publish, metrics.Error)

			return err
		}
	}

	p.metrics.Total(publish, metrics.Ok)

	return nil
}

func (p *Publisher) publishOne(ctx context.Context, e domain.Event) error {
	spanCtx, span := tracer.Start(tracer.Extract(ctx, e.Headers), "nats publish "+e.Subject,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("nats"),
			semconv.MessagingDestinationName(e.Subject),
			semconv.MessagingMessageID(e.ID.String()),
		),
	)
	defer span.End()

	headers := make(map[string]string, len(e.Headers))
	maps.Copy(headers, e.Headers)
	tracer.Inject(spanCtx, headers)

	msg := &nats.Msg{Subject: e.Subject, Data: e.Payload, Header: make(nats.Header, len(headers))}
	for k, v := range headers {
		msg.Header.Set(k, v)
	}

	_, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(e.ID.String()))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return fmt.Errorf("js.PublishMsg %s: %w", e.Subject, err)
	}

	return nil
}
