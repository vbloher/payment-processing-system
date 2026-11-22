package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

// HandleEvent records the event in the inbox, creates its notification once, and delivers it.
// A redelivered event whose notification is still pending is sent again; the receiver
// deduplicates on the Idempotency-Key. A sent notification is never sent twice.
func (u *UseCase) HandleEvent(ctx context.Context, e events.Envelope) error {
	ctx, span := tracer.Start(ctx, "usecase HandleEvent")
	defer span.End()

	err := u.tx.Wrap(ctx, func(ctx context.Context) error {
		inserted, err := u.postgres.SaveInbox(ctx, e.ID, string(e.Type))
		if err != nil {
			return fmt.Errorf("postgres.SaveInbox: %w", err)
		}

		if !inserted {
			return nil
		}

		n, err := domain.NewNotification(e)
		if err != nil {
			return fmt.Errorf("domain.NewNotification: %w", err)
		}

		err = u.postgres.CreateNotification(ctx, n)
		if err != nil {
			return fmt.Errorf("postgres.CreateNotification: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	n, err := u.postgres.GetNotificationByEventID(ctx, e.ID)
	if err != nil {
		return fmt.Errorf("postgres.GetNotificationByEventID: %w", err)
	}

	if n.Status == domain.StatusSent {
		return nil
	}

	err = u.sender.Send(ctx, n)
	if err != nil {
		return fmt.Errorf("sender.Send: %w", err)
	}

	err = u.postgres.MarkNotificationSent(ctx, n.ID)
	if err != nil {
		return fmt.Errorf("postgres.MarkNotificationSent: %w", err)
	}

	return nil
}
