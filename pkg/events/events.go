// Package events defines the event envelope that services exchange over NATS JetStream.
// It is a contract: the payment service publishes these events after commit and the
// notification service consumes them. It holds no domain logic.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Type names an event kind. It is also the suffix of the event's NATS subject.
type Type string

const (
	TypePaymentSucceeded      Type = "payment.succeeded"
	TypePaymentFailed         Type = "payment.failed"
	TypePaymentReviewRequired Type = "payment.review_required"
	TypeDiscrepancyFound      Type = "reconciliation.discrepancy_found"
)

// Subject returns the JetStream subject the event is published to.
func (t Type) Subject() string {
	return "events." + string(t)
}

// Envelope is the wire format of every event.
type Envelope struct {
	ID         uuid.UUID       `json:"id"`
	Type       Type            `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	PaymentID  uuid.UUID       `json:"payment_id"` // uuid.Nil when the event has no payment
	Data       json.RawMessage `json:"data"`
}

// PaymentData is the payload of payment.* events.
type PaymentData struct {
	AccountID     uuid.UUID `json:"account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
}

// DiscrepancyData is the payload of reconciliation.discrepancy_found events.
type DiscrepancyData struct {
	DiscrepancyID uuid.UUID `json:"discrepancy_id"`
	Kind          string    `json:"kind"`
	Reference     string    `json:"reference"`
	Details       string    `json:"details,omitempty"`
}

// ErrInvalidEnvelope is returned by Unmarshal for input that is not a usable envelope.
var ErrInvalidEnvelope = errors.New("invalid event envelope")

// New builds an envelope with a fresh ID and the current UTC time. data is marshaled into Data.
func New(t Type, paymentID uuid.UUID, data any) (Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("events.New: marshal data: %w", err)
	}

	return Envelope{
		ID:         uuid.New(),
		Type:       t,
		OccurredAt: time.Now().UTC(),
		PaymentID:  paymentID,
		Data:       raw,
	}, nil
}

// Marshal encodes the envelope as JSON.
func (e Envelope) Marshal() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("events.Envelope.Marshal: %w", err)
	}

	return b, nil
}

// Unmarshal decodes an envelope. It returns an error wrapping ErrInvalidEnvelope when the
// input is not JSON, or when the ID is zero or the Type is empty.
func Unmarshal(b []byte) (Envelope, error) {
	var e Envelope

	err := json.Unmarshal(b, &e)
	if err != nil {
		return Envelope{}, fmt.Errorf("events.Unmarshal: %w: %w", ErrInvalidEnvelope, err)
	}

	if e.ID == uuid.Nil {
		return Envelope{}, fmt.Errorf("events.Unmarshal: %w: zero id", ErrInvalidEnvelope)
	}

	if e.Type == "" {
		return Envelope{}, fmt.Errorf("events.Unmarshal: %w: empty type", ErrInvalidEnvelope)
	}

	return e, nil
}
