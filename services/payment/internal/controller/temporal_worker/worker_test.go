package temporal_worker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/mocks"

	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/temporal_worker"
)

// An invalid config is rejected before any worker starts or any schedule is created: the client mock
// has no expectations, so any call on it fails the test.
func Test_New_RejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *temporal_worker.Config)
	}{
		{"zero interval", func(c *temporal_worker.Config) { c.ReconciliationInterval = 0 }},
		{"zero lookback", func(c *temporal_worker.Config) { c.ReconciliationLookback = 0 }},
		{"negative lag", func(c *temporal_worker.Config) { c.ReconciliationLag = -time.Second }},
		{"lag that leaves no window", func(c *temporal_worker.Config) {
			c.ReconciliationLag = c.ReconciliationLookback
		}},
		{"zero stuck threshold", func(c *temporal_worker.Config) { c.ReconciliationStuckAfter = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := config
			tt.mutate(&c)

			w, err := temporal_worker.New(t.Context(), mocks.NewClient(t), c, nil)
			require.ErrorIs(t, err, temporal_worker.ErrInvalidConfig)
			require.Nil(t, w)
		})
	}
}
