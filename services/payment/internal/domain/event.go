package domain

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
)

// Event is an outbox message. Trace headers are added by the outbox adapter.
type Event struct {
	ID      uuid.UUID // JetStream message ID and inbox key
	Subject string
	Payload []byte
	Headers map[string]string
}

// ToEvent builds the outbox event of type t describing the payment's current state.
func (p Payment) ToEvent(t events.Type) (Event, error) {
	env, err := events.New(t, p.ID, events.PaymentData{
		AccountID:     p.AccountID,
		Amount:        p.Amount,
		Currency:      p.Currency,
		Status:        string(p.Status),
		FailureReason: string(p.FailureReason),
	})
	if err != nil {
		return Event{}, fmt.Errorf("domain.Payment.ToEvent: %w", err)
	}

	payload, err := env.Marshal()
	if err != nil {
		return Event{}, fmt.Errorf("domain.Payment.ToEvent: %w", err)
	}

	return Event{ID: env.ID, Subject: t.Subject(), Payload: payload, Headers: map[string]string{}}, nil
}
