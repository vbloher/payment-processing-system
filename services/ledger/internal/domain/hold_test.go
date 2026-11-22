package domain_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

func TestNewHold(t *testing.T) {
	h, err := domain.NewHold(uuid.New(), uuid.New(), 10, "EUR")
	require.NoError(t, err)
	require.Equal(t, domain.HoldActive, h.Status)

	_, err = domain.NewHold(uuid.New(), uuid.New(), 0, "EUR")
	require.ErrorIs(t, err, domain.ErrValidation)
}

func TestHold_Transitions(t *testing.T) {
	tests := []struct {
		name        string
		start       domain.HoldStatus
		call        func(*domain.Hold) (bool, error)
		wantChanged bool
		wantStatus  domain.HoldStatus
		wantErr     error
	}{
		{"active capture", domain.HoldActive, (*domain.Hold).Capture, true, domain.HoldCaptured, nil},
		{"captured capture", domain.HoldCaptured, (*domain.Hold).Capture, false, domain.HoldCaptured, nil},
		{"released capture", domain.HoldReleased, (*domain.Hold).Capture, false, domain.HoldReleased, domain.ErrHoldConflict},
		{"active release", domain.HoldActive, (*domain.Hold).Release, true, domain.HoldReleased, nil},
		{"released release", domain.HoldReleased, (*domain.Hold).Release, false, domain.HoldReleased, nil},
		{"captured release", domain.HoldCaptured, (*domain.Hold).Release, false, domain.HoldCaptured, domain.ErrHoldConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := domain.NewHold(uuid.New(), uuid.New(), 10, "EUR")
			require.NoError(t, err)
			h.Status = tt.start

			changed, err := tt.call(&h)
			require.Equal(t, tt.wantChanged, changed)
			require.Equal(t, tt.wantStatus, h.Status)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestHold_SameRequest(t *testing.T) {
	acc := uuid.New()
	h, err := domain.NewHold(uuid.New(), acc, 10, "EUR")
	require.NoError(t, err)

	require.True(t, h.SameRequest(acc, 10, "EUR"))
	require.False(t, h.SameRequest(uuid.New(), 10, "EUR"))
	require.False(t, h.SameRequest(acc, 11, "EUR"))
	require.False(t, h.SameRequest(acc, 10, "USD"))
}
