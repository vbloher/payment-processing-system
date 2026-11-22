package notification_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/notification"
	"github.com/vbloher/payment-processing-system/services/notification/config"
)

// Run checks the configuration before it connects to anything, including configs built by hand.
func Test_Run_RejectsInvalidConfig(t *testing.T) {
	var c config.Config
	c.Consumer.AckWait = 30 * time.Second
	c.Webhook.Timeout = 0

	require.ErrorIs(t, notification.Run(t.Context(), c), config.ErrInvalidConfig)
}
