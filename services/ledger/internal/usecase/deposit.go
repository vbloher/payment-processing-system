package usecase

import (
	"context"
	"fmt"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

// Deposit credits a customer account against bank_cash. It is idempotent on the idempotency key.
// Lock order: customer account, bank_cash.
func (u *UseCase) Deposit(ctx context.Context, in dto.DepositInput) error {
	ctx, span := tracer.Start(ctx, "usecase Deposit")
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

		bankCash, err := u.postgres.GetBankCashForUpdate(ctx, in.Currency)
		if err != nil {
			return fmt.Errorf("postgres.GetBankCashForUpdate: %w", err)
		}

		posting, err := domain.NewTransaction(domain.KindDeposit, in.IdempotencyKey, in.Currency,
			domain.Entry{AccountID: bankCash.ID, Side: domain.Debit, Amount: in.Amount},
			domain.Entry{AccountID: account.ID, Side: domain.Credit, Amount: in.Amount},
		)
		if err != nil {
			return fmt.Errorf("domain.NewTransaction: %w", err)
		}

		created, err := u.postgres.CreateTransaction(ctx, posting)
		if err != nil {
			return fmt.Errorf("postgres.CreateTransaction: %w", err)
		}

		if !created {
			return nil
		}

		return u.postDeposit(ctx, account, bankCash, in)
	})
	if err != nil {
		return fmt.Errorf("transaction.Wrap: %w", err)
	}

	return nil
}

func (u *UseCase) postDeposit(
	ctx context.Context, account, bankCash domain.Account, in dto.DepositInput,
) error {
	err := account.Post(domain.Credit, in.Amount, in.Currency)
	if err != nil {
		return fmt.Errorf("account.Post: %w", err)
	}

	err = bankCash.Post(domain.Debit, in.Amount, in.Currency)
	if err != nil {
		return fmt.Errorf("bankCash.Post: %w", err)
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
