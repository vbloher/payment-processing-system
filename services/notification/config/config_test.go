package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/notification/config"
)

// setRequired sets the variables that have no defaults.
func setRequired(t *testing.T) {
	t.Helper()

	for name, value := range map[string]string{
		"POSTGRES_USER":     "u",
		"POSTGRES_PASSWORD": "p",
		"POSTGRES_PORT":     "5432",
		"POSTGRES_HOST":     "localhost",
		"POSTGRES_DB_NAME":  "notification",
		"NATS_URL":          "nats://localhost:4222",
	} {
		t.Setenv(name, value)
	}
}

func Test_New_Defaults(t *testing.T) {
	// Run from an empty directory so no .env file is loaded.
	t.Chdir(t.TempDir())
	setRequired(t)

	for _, name := range []string{
		"HTTP_PORT", "NATS_CONSUMER_DURABLE", "NATS_CONSUMER_MAX_DELIVER", "NATS_CONSUMER_RETRY_DELAY",
		"NATS_CONSUMER_ACK_WAIT", "NOTIFICATION_WEBHOOK_URL", "NOTIFICATION_WEBHOOK_TIMEOUT",
	} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "8082", c.HTTP.Port)
	require.Equal(t, "notification", c.Consumer.Durable)
	require.Equal(t, 5, c.Consumer.MaxDeliver)
	require.Equal(t, time.Second, c.Consumer.RetryDelay)
	require.Equal(t, 30*time.Second, c.Consumer.AckWait)
	require.Empty(t, c.Webhook.URL)
	require.Equal(t, 5*time.Second, c.Webhook.Timeout)
}

func Test_New_Override(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequired(t)
	t.Setenv("HTTP_PORT", "9000")
	t.Setenv("NOTIFICATION_WEBHOOK_URL", "http://localhost:9999/hook")
	t.Setenv("NATS_CONSUMER_MAX_DELIVER", "7")

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "9000", c.HTTP.Port)
	require.Equal(t, "http://localhost:9999/hook", c.Webhook.URL)
	require.Equal(t, 7, c.Consumer.MaxDeliver)
}

func Test_Validate(t *testing.T) {
	// AckWait 10s leaves the handler 8s per delivery; the rest is kept to settle the message.
	tests := []struct {
		name           string
		webhookTimeout time.Duration
		wantErr        bool
	}{
		{name: "below the handler timeout", webhookTimeout: 7 * time.Second},
		{name: "no timeout", webhookTimeout: 0, wantErr: true},
		{name: "negative", webhookTimeout: -time.Second, wantErr: true},
		{name: "equal to the handler timeout", webhookTimeout: 8 * time.Second, wantErr: true},
		{name: "equal to the ack wait", webhookTimeout: 10 * time.Second, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c config.Config
			c.Consumer.AckWait = 10 * time.Second
			c.Webhook.Timeout = tt.webhookTimeout

			err := c.Validate()
			if tt.wantErr {
				require.ErrorIs(t, err, config.ErrInvalidConfig)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_New_RequiresNATS(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequired(t)
	t.Setenv("NATS_URL", "")
	require.NoError(t, os.Unsetenv("NATS_URL"))

	_, err := config.New()
	require.ErrorContains(t, err, "NATS_URL")
}
