package domain_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

func TestNewAccount(t *testing.T) {
	c, err := domain.NewCustomerAccount("EUR")
	require.NoError(t, err)
	require.NotEmpty(t, c.ID)
	require.Equal(t, domain.KindCustomer, c.Kind)
	require.Equal(t, domain.Credit, c.NormalSide)
	require.False(t, c.CreatedAt.IsZero())
	require.Equal(t, c.CreatedAt, c.UpdatedAt)

	b, err := domain.NewBankCashAccount("EUR")
	require.NoError(t, err)
	require.Equal(t, domain.KindBankCash, b.Kind)
	require.Equal(t, domain.Debit, b.NormalSide)

	_, err = domain.NewCustomerAccount("eur")
	require.ErrorIs(t, err, domain.ErrValidation)
	_, err = domain.NewBankCashAccount("")
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestAccount_Post(t *testing.T) {
	tests := []struct {
		name        string
		bankCash    bool
		balance     int64
		held        int64
		side        domain.Side
		amount      int64
		currency    string
		wantBalance int64
		wantErr     error
	}{
		{"customer credit", false, 0, 0, domain.Credit, 100, "EUR", 100, nil},
		{"customer debit", false, 100, 0, domain.Debit, 30, "EUR", 70, nil},
		{"customer debit over balance", false, 100, 0, domain.Debit, 101, "EUR", 100, domain.ErrInsufficientFunds},
		{"customer debit into held", false, 100, 80, domain.Debit, 30, "EUR", 100, domain.ErrInsufficientFunds},
		{"customer currency mismatch", false, 100, 0, domain.Credit, 1, "USD", 100, domain.ErrCurrencyMismatch},
		{"customer overflow", false, math.MaxInt64 - 1, 0, domain.Credit, 2, "EUR", math.MaxInt64 - 1, domain.ErrValidation},
		{"bank cash debit", true, 0, 0, domain.Debit, 100, "EUR", 100, nil},
		{"bank cash credit", true, 100, 0, domain.Credit, 30, "EUR", 70, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var a domain.Account
			var err error
			if tt.bankCash {
				a, err = domain.NewBankCashAccount("EUR")
			} else {
				a, err = domain.NewCustomerAccount("EUR")
			}
			require.NoError(t, err)
			a.Balance, a.Held = tt.balance, tt.held
			before := a

			err = a.Post(tt.side, tt.amount, tt.currency)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Equal(t, before, a, "account must be unchanged on error")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantBalance, a.Balance)
			require.Equal(t, tt.held, a.Held)
		})
	}
}

func TestAccount_Reserve(t *testing.T) {
	a, err := domain.NewCustomerAccount("EUR")
	require.NoError(t, err)
	a.Balance = 100

	require.NoError(t, a.Reserve(60, "EUR"))
	require.Equal(t, int64(60), a.Held)
	require.Equal(t, int64(40), a.Available())

	before := a
	require.ErrorIs(t, a.Reserve(41, "EUR"), domain.ErrInsufficientFunds)
	require.Equal(t, before, a)
	require.ErrorIs(t, a.Reserve(1, "USD"), domain.ErrCurrencyMismatch)
	require.Equal(t, before, a)

	a.Unreserve(60)
	require.Equal(t, int64(0), a.Held)
}
