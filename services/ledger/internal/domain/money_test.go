package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

func TestValidateMoney(t *testing.T) {
	tests := []struct {
		name     string
		amount   int64
		currency string
		wantErr  error
	}{
		{"minimum", 1, "EUR", nil},
		{"maximum", 1_000_000_000_000, "EUR", nil},
		{"zero", 0, "EUR", domain.ErrValidation},
		{"negative", -1, "EUR", domain.ErrValidation},
		{"above maximum", 1_000_000_000_001, "EUR", domain.ErrValidation},
		{"lowercase currency", 1, "eur", domain.ErrValidation},
		{"short currency", 1, "EU", domain.ErrValidation},
		{"long currency", 1, "EURO", domain.ErrValidation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := domain.ValidateMoney(tt.amount, tt.currency)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestIsInvalidInput(t *testing.T) {
	require.True(t, domain.IsInvalidInput(domain.ErrValidation))
	require.True(t, domain.IsInvalidInput(domain.ErrCurrencyMismatch))
	require.True(t, domain.IsInvalidInput(domain.ErrUnbalanced))
	require.False(t, domain.IsInvalidInput(domain.ErrNotFound))
	require.False(t, domain.IsInvalidInput(domain.ErrInsufficientFunds))
}
