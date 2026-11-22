package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

//go:generate go tool mockery

type Transactor interface {
	Wrap(ctx context.Context, fn func(ctx context.Context) error) error
}

type Postgres interface {
	// SaveInbox reports false when the event was already recorded.
	SaveInbox(ctx context.Context, eventID uuid.UUID, eventType string) (inserted bool, err error)
	CreateNotification(ctx context.Context, n domain.Notification) error
	GetNotificationByEventID(ctx context.Context, eventID uuid.UUID) (domain.Notification, error)
	MarkNotificationSent(ctx context.Context, id uuid.UUID) error
}

// Sender delivers a notification. Delivery is at-least-once: implementations send
// the notification's event ID as the Idempotency-Key.
type Sender interface {
	Send(ctx context.Context, n domain.Notification) error
}

type UseCase struct {
	tx       Transactor
	postgres Postgres
	sender   Sender
}

func New(tx Transactor, postgres Postgres, sender Sender) *UseCase {
	return &UseCase{
		tx:       tx,
		postgres: postgres,
		sender:   sender,
	}
}
