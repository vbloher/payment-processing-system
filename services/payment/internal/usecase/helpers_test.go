package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase/mocks"
)

const Any = mock.Anything

func newTransactor(t *testing.T) *mocks.Transactor {
	t.Helper()

	tx := mocks.NewTransactor(t)
	tx.EXPECT().Wrap(Any, Any).RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	}).Maybe()

	return tx
}

// deps bundles the mocks of a use case under test.
type deps struct {
	postgres     *mocks.Postgres
	ledger       *mocks.Ledger
	bank         *mocks.Bank
	publisher    *mocks.Publisher
	orchestrator *mocks.Orchestrator
}

func newUseCase(t *testing.T) (*usecase.UseCase, deps) {
	t.Helper()

	d := deps{
		postgres:     mocks.NewPostgres(t),
		ledger:       mocks.NewLedger(t),
		bank:         mocks.NewBank(t),
		publisher:    mocks.NewPublisher(t),
		orchestrator: mocks.NewOrchestrator(t),
	}

	return usecase.New(newTransactor(t), d.postgres, d.ledger, d.bank, d.publisher, d.orchestrator), d
}

func newPayment(status domain.Status) domain.Payment {
	return domain.Payment{
		ID:             uuid.New(),
		IdempotencyKey: "key-1",
		AccountID:      uuid.New(),
		Amount:         3000,
		Currency:       "EUR",
		Beneficiary:    "ACME",
		Status:         status,
	}
}
