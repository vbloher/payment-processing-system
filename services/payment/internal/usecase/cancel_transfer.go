package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// CancelTransfer asks the bank to cancel the transfer. The bank answers with the definitive state,
// which may be succeeded when the transfer won the race.
func (u *UseCase) CancelTransfer(ctx context.Context, paymentID uuid.UUID) (domain.BankOutcome, error) {
	ctx, span := tracer.Start(ctx, "usecase CancelTransfer")
	defer span.End()

	payment, err := u.postgres.GetPayment(ctx, paymentID)
	if err != nil {
		return "", fmt.Errorf("postgres.GetPayment: %w", err)
	}

	transfer, err := u.bank.Cancel(ctx, payment.ID.String())
	if err != nil {
		return "", fmt.Errorf("bank.Cancel: %w", err)
	}

	err = u.storeBankReference(ctx, payment, transfer)
	if err != nil {
		return "", err
	}

	return transfer.Status, nil
}

// storeBankReference records the bank's transfer ID when the payment has none. It re-reads the
// payment under a row lock and writes only that field, so a concurrent status change is not
// overwritten with the stale copy loaded before the bank call.
func (u *UseCase) storeBankReference(ctx context.Context, payment domain.Payment, transfer domain.BankTransfer) error {
	if payment.BankReference != "" || transfer.ID == "" {
		return nil
	}

	err := u.tx.Wrap(ctx, func(ctx context.Context) error {
		locked, err := u.postgres.GetPaymentForUpdate(ctx, payment.ID)
		if err != nil {
			return fmt.Errorf("postgres.GetPaymentForUpdate: %w", err)
		}

		if locked.BankReference != "" {
			return nil
		}

		locked.BankReference = transfer.ID

		err = u.postgres.UpdatePayment(ctx, locked)
		if err != nil {
			return fmt.Errorf("postgres.UpdatePayment: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	return nil
}
