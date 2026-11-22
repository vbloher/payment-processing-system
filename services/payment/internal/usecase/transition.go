package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// transition is the only place a payment's status changes. In one transaction it locks the payment
// and moves it to next. A transition to the current status writes nothing, so a retried step emits
// no second event. Otherwise mutate runs, the payment is saved and, when event is not empty, the
// outbox event is saved in the same transaction.
func (u *UseCase) transition(
	ctx context.Context,
	paymentID uuid.UUID,
	next domain.Status,
	event events.Type,
	mutate func(p *domain.Payment),
) error {
	err := u.tx.Wrap(ctx, func(ctx context.Context) error {
		payment, err := u.postgres.GetPaymentForUpdate(ctx, paymentID)
		if err != nil {
			return fmt.Errorf("postgres.GetPaymentForUpdate: %w", err)
		}

		changed, err := payment.TransitionTo(next)
		if err != nil {
			return fmt.Errorf("payment.TransitionTo: %w", err)
		}

		if !changed {
			return nil
		}

		if mutate != nil {
			mutate(&payment)
		}

		err = u.postgres.UpdatePayment(ctx, payment)
		if err != nil {
			return fmt.Errorf("postgres.UpdatePayment: %w", err)
		}

		if event == "" {
			return nil
		}

		outbox, err := payment.ToEvent(event)
		if err != nil {
			return fmt.Errorf("payment.ToEvent: %w", err)
		}

		err = u.postgres.SaveOutbox(ctx, outbox)
		if err != nil {
			return fmt.Errorf("postgres.SaveOutbox: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	return nil
}
