package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusSent    Status = "sent"
)

// Notification is a webhook to deliver for one event.
type Notification struct {
	ID        uuid.UUID
	EventID   uuid.UUID
	EventType string
	PaymentID uuid.UUID
	Payload   []byte // the envelope as JSON
	Status    Status
	CreatedAt time.Time
}

// NewNotification builds a pending notification for the event.
func NewNotification(e events.Envelope) (Notification, error) {
	payload, err := e.Marshal()
	if err != nil {
		return Notification{}, fmt.Errorf("envelope.Marshal: %w", err)
	}

	return Notification{
		ID:        uuid.New(),
		EventID:   e.ID,
		EventType: string(e.Type),
		PaymentID: e.PaymentID,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: time.Now().UTC(),
	}, nil
}
