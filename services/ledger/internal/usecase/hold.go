package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

// Hold reserves funds for a payment. It is idempotent on the payment ID.
func (u *UseCase) Hold(ctx context.Context, in dto.HoldInput) error {
	ctx, span := tracer.Start(ctx, "usecase Hold")
	defer span.End()

	err := domain.ValidateMoney(in.Amount, in.Currency)
	if err != nil {
		return fmt.Errorf("domain.ValidateMoney: %w", err)
	}

	err = u.tx.Wrap(ctx, func(ctx context.Context) error {
		account, err := u.postgres.GetAccountForUpdate(ctx, in.AccountID)
		if err != nil {
			return fmt.Errorf("postgres.GetAccountForUpdate: %w", err)
		}

		if account.Kind != domain.KindCustomer {
			return fmt.Errorf("account %s is not a customer account: %w", in.AccountID, domain.ErrNotFound)
		}

		existing, err := u.postgres.GetHold(ctx, in.PaymentID)

		switch {
		case err == nil:
			if existing.SameRequest(in.AccountID, in.Amount, in.Currency) {
				return nil
			}

			return fmt.Errorf("hold %s: %w", in.PaymentID, domain.ErrIdempotencyConflict)
		case !errors.Is(err, domain.ErrNotFound):
			return fmt.Errorf("postgres.GetHold: %w", err)
		}

		return u.reserve(ctx, account, in)
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	return nil
}

func (u *UseCase) reserve(ctx context.Context, account domain.Account, in dto.HoldInput) error {
	hold, err := domain.NewHold(in.PaymentID, in.AccountID, in.Amount, in.Currency)
	if err != nil {
		return fmt.Errorf("domain.NewHold: %w", err)
	}

	err = account.Reserve(in.Amount, in.Currency)
	if err != nil {
		return fmt.Errorf("account.Reserve: %w", err)
	}

	err = u.postgres.CreateHold(ctx, hold)
	if err != nil {
		return fmt.Errorf("postgres.CreateHold: %w", err)
	}

	err = u.postgres.UpdateAccountBalances(ctx, account)
	if err != nil {
		return fmt.Errorf("postgres.UpdateAccountBalances: %w", err)
	}

	return nil
}
