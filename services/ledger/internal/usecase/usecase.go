package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

//go:generate go tool mockery

type Transactor interface {
	Wrap(ctx context.Context, fn func(ctx context.Context) error) error
}

//nolint:interfacebloat // one port per datastore, as in the template.
type Postgres interface {
	CreateAccount(ctx context.Context, account domain.Account) error
	GetAccount(ctx context.Context, id uuid.UUID) (domain.Account, error)
	GetAccountForUpdate(ctx context.Context, id uuid.UUID) (domain.Account, error)
	// GetBankCashForUpdate creates the bank_cash account of the currency when absent.
	GetBankCashForUpdate(ctx context.Context, currency string) (domain.Account, error)
	UpdateAccountBalances(ctx context.Context, account domain.Account) error
	// CreateTransaction reports false when a transaction with the same (kind, reference) exists.
	CreateTransaction(ctx context.Context, tx domain.Transaction) (created bool, err error)
	CreateHold(ctx context.Context, hold domain.Hold) error
	GetHold(ctx context.Context, paymentID uuid.UUID) (domain.Hold, error)
	GetHoldForUpdate(ctx context.Context, paymentID uuid.UUID) (domain.Hold, error)
	UpdateHoldStatus(ctx context.Context, hold domain.Hold) error
	GetHolds(ctx context.Context, paymentIDs []uuid.UUID) ([]domain.Hold, error)
}

type UseCase struct {
	tx       Transactor
	postgres Postgres
}

func New(tx Transactor, postgres Postgres) *UseCase {
	return &UseCase{
		tx:       tx,
		postgres: postgres,
	}
}
