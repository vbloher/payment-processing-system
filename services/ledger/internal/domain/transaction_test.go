package domain_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

func TestNewTransaction(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	debit := func(n int64) domain.Entry { return domain.Entry{AccountID: a, Side: domain.Debit, Amount: n} }
	credit := func(n int64) domain.Entry { return domain.Entry{AccountID: b, Side: domain.Credit, Amount: n} }

	tests := []struct {
		name      string
		reference string
		entries   []domain.Entry
		wantErr   error
	}{
		{"balanced", "ref-1", []domain.Entry{debit(100), credit(100)}, nil},
		{"unbalanced", "ref-1", []domain.Entry{debit(100), credit(99)}, domain.ErrUnbalanced},
		{"one entry", "ref-1", []domain.Entry{debit(100)}, domain.ErrValidation},
		{"zero amount", "ref-1", []domain.Entry{debit(0), credit(0)}, domain.ErrValidation},
		{"empty reference", "", []domain.Entry{debit(100), credit(100)}, domain.ErrValidation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := domain.NewTransaction(domain.KindDeposit, tt.reference, "EUR", tt.entries...)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, tx.ID)
			require.Equal(t, domain.KindDeposit, tx.Kind)
			require.Equal(t, tt.reference, tx.Reference)
			require.Equal(t, "EUR", tx.Currency)
			require.Equal(t, tt.entries, tx.Entries)
			require.False(t, tx.CreatedAt.IsZero())
		})
	}
}
