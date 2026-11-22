package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
)

func (u *UseCase) SendOutbox(ctx context.Context, limit int) (int, error) {
	ctx, span := tracer.Start(ctx, "usecase SendOutbox")
	defer span.End()

	var sent int

	// Read-and-delete and publish share one transaction: if NATS fails, the events stay in the outbox.
	err := u.tx.Wrap(ctx, func(ctx context.Context) error {
		events, err := u.postgres.ReadOutbox(ctx, limit)
		if err != nil {
			return fmt.Errorf("postgres.ReadOutbox: %w", err)
		}

		if len(events) == 0 {
			return nil
		}

		err = u.publisher.Publish(ctx, events...)
		if err != nil {
			return fmt.Errorf("publisher.Publish: %w", err)
		}

		sent = len(events)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("transaction.Wrap: %w", err)
	}

	return sent, nil
}
