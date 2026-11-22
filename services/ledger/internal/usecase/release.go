package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

// Release gives an active hold back to the available balance. Repeating it is a no-op.
// Lock order: customer account, hold.
func (u *UseCase) Release(ctx context.Context, in dto.ReleaseInput) error {
	ctx, span := tracer.Start(ctx, "usecase Release")
	defer span.End()

	// Unlocked read: only to learn which account to lock first.
	probe, err := u.postgres.GetHold(ctx, in.PaymentID)
	if err != nil {
		return fmt.Errorf("postgres.GetHold: %w", err)
	}

	err = u.tx.Wrap(ctx, func(ctx context.Context) error {
		account, err := u.postgres.GetAccountForUpdate(ctx, probe.AccountID)
		if err != nil {
			return fmt.Errorf("postgres.GetAccountForUpdate: %w", err)
		}

		hold, err := u.postgres.GetHoldForUpdate(ctx, in.PaymentID)
		if err != nil {
			return fmt.Errorf("postgres.GetHoldForUpdate: %w", err)
		}

		changed, err := hold.Release()
		if err != nil {
			return fmt.Errorf("hold.Release: %w", err)
		}

		if !changed {
			return nil
		}

		account.Unreserve(hold.Amount)

		err = u.postgres.UpdateHoldStatus(ctx, hold)
		if err != nil {
			return fmt.Errorf("postgres.UpdateHoldStatus: %w", err)
		}

		err = u.postgres.UpdateAccountBalances(ctx, account)
		if err != nil {
			return fmt.Errorf("postgres.UpdateAccountBalances: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	return nil
}
