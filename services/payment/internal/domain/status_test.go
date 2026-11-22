package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func TestPayment_TransitionTo(t *testing.T) {
	const (
		c = domain.StatusCreated
		p = domain.StatusProcessing
		s = domain.StatusSucceeded
		f = domain.StatusFailed
		r = domain.StatusReview
	)

	tests := []struct {
		from, to    domain.Status
		wantChanged bool
		wantErr     error
	}{
		{c, p, true, nil},
		{c, c, false, nil},
		{c, s, false, domain.ErrInvalidTransition},
		{c, f, false, domain.ErrInvalidTransition},
		{p, s, true, nil},
		{p, f, true, nil},
		{p, r, true, nil},
		{p, p, false, nil},
		{p, c, false, domain.ErrInvalidTransition},
		{r, s, true, nil},
		{r, f, true, nil},
		{r, r, false, nil},
		{r, p, false, domain.ErrInvalidTransition},
		{s, s, false, nil},
		{s, f, false, domain.ErrInvalidTransition},
		{f, f, false, nil},
		{f, s, false, domain.ErrInvalidTransition},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"_to_"+string(tt.to), func(t *testing.T) {
			pay := domain.Payment{Status: tt.from}

			changed, err := pay.TransitionTo(tt.to)

			require.Equal(t, tt.wantChanged, changed)
			require.ErrorIs(t, err, tt.wantErr)

			if tt.wantChanged {
				require.Equal(t, tt.to, pay.Status)
				require.False(t, pay.UpdatedAt.IsZero())
			} else {
				require.Equal(t, tt.from, pay.Status)
				require.True(t, pay.UpdatedAt.IsZero())
			}
		})
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	require.True(t, domain.StatusSucceeded.IsTerminal())
	require.True(t, domain.StatusFailed.IsTerminal())
	require.False(t, domain.StatusCreated.IsTerminal())
	require.False(t, domain.StatusProcessing.IsTerminal())
	require.False(t, domain.StatusReview.IsTerminal())
}

func TestErrorClassification(t *testing.T) {
	require.True(t, domain.IsInvalidInput(domain.ErrValidation))
	require.True(t, domain.IsInvalidInput(domain.ErrIdempotencyKeyRequired))
	require.False(t, domain.IsInvalidInput(domain.ErrNotFound))

	for _, e := range []error{domain.ErrNotFound, domain.ErrInvalidTransition, domain.ErrValidation, domain.ErrLedgerConflict} {
		require.True(t, domain.IsFatal(e))
	}

	require.False(t, domain.IsFatal(domain.ErrBankUnavailable))
	require.False(t, domain.IsFatal(domain.ErrInsufficientFunds))
}
