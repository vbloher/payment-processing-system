package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/codes"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

// maxBody bounds how much of an answer's body is read; a failed answer's head goes into the error.
const maxBody = 256

var ErrUnexpectedStatus = errors.New("webhook: unexpected status")

type Config struct {
	URL     string        `env:"NOTIFICATION_WEBHOOK_URL"`
	Timeout time.Duration `env:"NOTIFICATION_WEBHOOK_TIMEOUT" envDefault:"5s"`
}

// Sender posts notifications to the configured webhook.
type Sender struct {
	url    string
	client *http.Client
}

func New(c Config) *Sender {
	return &Sender{
		url: c.URL,
		client: &http.Client{
			Timeout:   c.Timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
			// A redirect would turn the POST into a GET without the payload: report it instead.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Send posts the notification's payload with its event ID as the Idempotency-Key. Any answer
// but 2xx is an error. Without a URL the notification is only logged.
func (s *Sender) Send(ctx context.Context, n domain.Notification) error {
	ctx, span := tracer.Start(ctx, "adapter webhook Send")
	defer span.End()

	if s.url == "" {
		log.Info().Str("event_id", n.EventID.String()).Str("event_type", n.EventType).
			Str("payment_id", n.PaymentID.String()).RawJSON("payload", n.Payload).
			Msg("webhook: no URL configured, notification logged")

		return nil
	}

	err := s.post(ctx, n)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	return nil
}

func (s *Sender) post(ctx context.Context, n domain.Notification) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(n.Payload))
	if err != nil {
		return fmt.Errorf("webhook.Send: http.NewRequestWithContext: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.EventID.String())

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook.Send: client.Do: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	// Only the head of the body is read: the receiver's answer is not ours to buffer.
	head, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}

	return fmt.Errorf("webhook.Send: %w: %s: %q", ErrUnexpectedStatus, resp.Status, head)
}
