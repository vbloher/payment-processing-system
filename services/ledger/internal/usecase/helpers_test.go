package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase/mocks"
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

func customer(balance, held int64) domain.Account {
	return domain.Account{
		ID:         uuid.New(),
		Kind:       domain.KindCustomer,
		Currency:   "EUR",
		NormalSide: domain.Credit,
		Balance:    balance,
		Held:       held,
	}
}

func bankCash(balance int64) domain.Account {
	return domain.Account{
		ID:         uuid.New(),
		Kind:       domain.KindBankCash,
		Currency:   "EUR",
		NormalSide: domain.Debit,
		Balance:    balance,
	}
}

func holdOf(paymentID uuid.UUID, account domain.Account, amount int64, status domain.HoldStatus) domain.Hold {
	return domain.Hold{
		PaymentID: paymentID,
		AccountID: account.ID,
		Amount:    amount,
		Currency:  "EUR",
		Status:    status,
	}
}

// accountWith matches an account saved with the given id, balance and held.
func accountWith(id uuid.UUID, balance, held int64) any {
	return mock.MatchedBy(func(a domain.Account) bool {
		return a.ID == id && a.Balance == balance && a.Held == held
	})
}
