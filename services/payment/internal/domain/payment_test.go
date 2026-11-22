package domain_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func TestNewPayment(t *testing.T) {
	acc := uuid.New()

	tests := []struct {
		name        string
		key         string
		account     uuid.UUID
		amount      int64
		currency    string
		beneficiary string
		description string
		wantErr     error
	}{
		{"valid", "key-1", acc, 1500, "EUR", "ACME", "invoice", nil},
		{"valid without description", "key-1", acc, 1, "USD", "ACME", "", nil},
		{"valid max amount", "key-1", acc, domain.MaxAmount, "USD", "ACME", "", nil},
		{"valid multibyte at limit", strings.Repeat("k", 255), acc, 1, "EUR", strings.Repeat("я", 140), strings.Repeat("я", 255), nil},
		{"empty key", "", acc, 1500, "EUR", "ACME", "", domain.ErrIdempotencyKeyRequired},
		{"long key", strings.Repeat("k", 256), acc, 1500, "EUR", "ACME", "", domain.ErrValidation},
		{"nil account", "key-1", uuid.Nil, 1500, "EUR", "ACME", "", domain.ErrValidation},
		{"zero amount", "key-1", acc, 0, "EUR", "ACME", "", domain.ErrValidation},
		{"negative amount", "key-1", acc, -1, "EUR", "ACME", "", domain.ErrValidation},
		{"amount over max", "key-1", acc, domain.MaxAmount + 1, "EUR", "ACME", "", domain.ErrValidation},
		{"lowercase currency", "key-1", acc, 1500, "eur", "ACME", "", domain.ErrValidation},
		{"empty beneficiary", "key-1", acc, 1500, "EUR", "", "", domain.ErrValidation},
		{"long beneficiary", "key-1", acc, 1500, "EUR", strings.Repeat("b", 141), "", domain.ErrValidation},
		{"long description", "key-1", acc, 1500, "EUR", "ACME", strings.Repeat("d", 256), domain.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := domain.NewPayment(tt.key, tt.account, tt.amount, tt.currency, tt.beneficiary, tt.description)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotEqual(t, uuid.Nil, p.ID)
			require.Equal(t, domain.StatusCreated, p.Status)
			require.Equal(t, tt.key, p.IdempotencyKey)
			require.Equal(t, domain.RequestHash(tt.account, tt.amount, tt.currency, tt.beneficiary, tt.description), p.RequestHash)
		})
	}
}

func TestRequestHash(t *testing.T) {
	acc := uuid.New()
	base := domain.RequestHash(acc, 100, "EUR", "ab", "c")

	require.Equal(t, base, domain.RequestHash(acc, 100, "EUR", "ab", "c"))
	require.Len(t, base, 64)

	tests := map[string]string{
		"account":     domain.RequestHash(uuid.New(), 100, "EUR", "ab", "c"),
		"amount":      domain.RequestHash(acc, 101, "EUR", "ab", "c"),
		"currency":    domain.RequestHash(acc, 100, "USD", "ab", "c"),
		"beneficiary": domain.RequestHash(acc, 100, "EUR", "abc", "c"),
		"description": domain.RequestHash(acc, 100, "EUR", "ab", "d"),
		"field split": domain.RequestHash(acc, 100, "EUR", "a", "bc"),
	}

	for name, h := range tests {
		t.Run(name, func(t *testing.T) {
			require.NotEqual(t, base, h)
		})
	}
}
