package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

//go:generate go tool mockery

type Transactor interface {
	Wrap(ctx context.Context, fn func(ctx context.Context) error) error
}

type Postgres interface {
	// CreatePayment reports false when a payment with the same idempotency key exists.
	CreatePayment(ctx context.Context, payment domain.Payment) (bool, error)
	GetPayment(ctx context.Context, id uuid.UUID) (domain.Payment, error)
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error)
	GetPaymentForUpdate(ctx context.Context, id uuid.UUID) (domain.Payment, error)
	UpdatePayment(ctx context.Context, payment domain.Payment) error
	GetPayments(ctx context.Context, ids []uuid.UUID) ([]domain.Payment, error)
	ListPaymentsByStatus(ctx context.Context, status domain.Status) ([]domain.Payment, error)
	ListPaymentsUpdatedBetween(ctx context.Context, from, to time.Time) ([]domain.Payment, error)
	SaveOutbox(ctx context.Context, events ...domain.Event) error
	ReadOutbox(ctx context.Context, limit int) ([]domain.Event, error)
}

type Ledger interface {
	Hold(ctx context.Context, paymentID, accountID uuid.UUID, amount int64, currency string) error
	Capture(ctx context.Context, paymentID uuid.UUID) error
	Release(ctx context.Context, paymentID uuid.UUID) error
	// GetHolds omits payments that have no hold from the map.
	GetHolds(ctx context.Context, paymentIDs []uuid.UUID) (map[uuid.UUID]domain.HoldState, error)
}

type Bank interface {
	Transfer(
		ctx context.Context, reference string, amount int64, currency, beneficiary string,
	) (domain.BankTransfer, error)
	Cancel(ctx context.Context, reference string) (domain.BankTransfer, error)
	// GetTransfer returns nil, nil when the bank has no such transfer.
	GetTransfer(ctx context.Context, reference string) (*domain.BankTransfer, error)
	GetStatement(ctx context.Context, from, to time.Time) ([]domain.BankTransfer, error)
}

type Publisher interface {
	Publish(ctx context.Context, events ...domain.Event) error
}

type Orchestrator interface {
	// StartPayment is idempotent.
	StartPayment(ctx context.Context, paymentID uuid.UUID) error
	SignalBankOutcome(ctx context.Context, paymentID uuid.UUID, outcome domain.BankOutcome) error
}

type UseCase struct {
	tx           Transactor
	postgres     Postgres
	ledger       Ledger
	bank         Bank
	publisher    Publisher
	orchestrator Orchestrator
}

func New(
	tx Transactor, postgres Postgres, ledger Ledger, bank Bank, publisher Publisher, orchestrator Orchestrator,
) *UseCase {
	return &UseCase{
		tx:           tx,
		postgres:     postgres,
		ledger:       ledger,
		bank:         bank,
		publisher:    publisher,
		orchestrator: orchestrator,
	}
}
