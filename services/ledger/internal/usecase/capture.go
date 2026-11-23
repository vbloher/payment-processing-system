package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

// Capture turns an active hold into a posted payment. Repeating it is a no-op.
// Lock order: customer account, hold, bank_cash.
func (u *UseCase) Capture(ctx context.Context, in dto.CaptureInput) error {
	ctx, span := tracer.Start(ctx, "usecase Capture")
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

		changed, err := hold.Capture()
		if err != nil {
			return fmt.Errorf("hold.Capture: %w", err)
		}

		if !changed {
			return nil
		}

		return u.postCapture(ctx, account, hold)
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	return nil
}

func (u *UseCase) postCapture(ctx context.Context, account domain.Account, hold domain.Hold) error {
	bankCash, err := u.postgres.GetBankCashForUpdate(ctx, hold.Currency)
	if err != nil {
		return fmt.Errorf("postgres.GetBankCashForUpdate: %w", err)
	}

	account.Unreserve(hold.Amount)

	err = account.Post(domain.Debit, hold.Amount, hold.Currency)
	if err != nil {
		return fmt.Errorf("account.Post: %w", err)
	}

	err = bankCash.Post(domain.Credit, hold.Amount, hold.Currency)
	if err != nil {
		return fmt.Errorf("bankCash.Post: %w", err)
	}

	posting, err := domain.NewTransaction(domain.KindPayment, hold.PaymentID.String(), hold.Currency,
		domain.Entry{AccountID: account.ID, Side: domain.Debit, Amount: hold.Amount},
		domain.Entry{AccountID: bankCash.ID, Side: domain.Credit, Amount: hold.Amount},
	)
	if err != nil {
		return fmt.Errorf("domain.NewTransaction: %w", err)
	}

	created, err := u.postgres.CreateTransaction(ctx, posting)
	if err != nil {
		return fmt.Errorf("postgres.CreateTransaction: %w", err)
	}

	// The posting exists while the hold is still active: moving the balances now would leave them
	// out of step with the entries. Unreachable in normal operation; fail before writing anything.
	if !created {
		return fmt.Errorf("payment %s is posted while its hold is active: %w", hold.PaymentID, domain.ErrHoldConflict)
	}

	err = u.postgres.UpdateHoldStatus(ctx, hold)
	if err != nil {
		return fmt.Errorf("postgres.UpdateHoldStatus: %w", err)
	}

	err = u.postgres.UpdateAccountBalances(ctx, account)
	if err != nil {
		return fmt.Errorf("postgres.UpdateAccountBalances: %w", err)
	}

	err = u.postgres.UpdateAccountBalances(ctx, bankCash)
	if err != nil {
		return fmt.Errorf("postgres.UpdateAccountBalances: %w", err)
	}

	return nil
}
