package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/payment/config"
)

// setRequired sets the variables that have no defaults.
func setRequired(t *testing.T) {
	t.Helper()

	for name, value := range map[string]string{
		"POSTGRES_USER":      "u",
		"POSTGRES_PASSWORD":  "p",
		"POSTGRES_PORT":      "5432",
		"POSTGRES_HOST":      "localhost",
		"POSTGRES_DB_NAME":   "payment",
		"NATS_URL":           "nats://localhost:4222",
		"TEMPORAL_HOST_PORT": "localhost:7233",
		"LEDGER_ADDR":        "localhost:50051",
		"BANK_URL":           "http://localhost:8090/api/v1",
	} {
		t.Setenv(name, value)
	}
}

func Test_New_Defaults(t *testing.T) {
	// Run from an empty directory so no .env file is loaded.
	t.Chdir(t.TempDir())
	setRequired(t)

	for _, name := range []string{
		"HTTP_PORT", "SAGA_ACTIVITY_TIMEOUT", "SAGA_RETRY_INITIAL_INTERVAL", "SAGA_RETRY_MAX_INTERVAL",
		"SAGA_BANK_MAX_ATTEMPTS", "OUTBOX_INTERVAL", "OUTBOX_LIMIT", "BANK_TIMEOUT", "TEMPORAL_NAMESPACE",
	} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "8080", c.HTTP.Port)
	require.Equal(t, "default", c.Temporal.Namespace)
	require.Equal(t, 3*time.Second, c.Bank.Timeout)
	require.Equal(t, 10*time.Second, c.Saga.ActivityTimeout)
	require.Equal(t, time.Second, c.Saga.RetryInitialInterval)
	require.Equal(t, 30*time.Second, c.Saga.RetryMaxInterval)
	require.EqualValues(t, 3, c.Saga.BankMaxAttempts)
	require.Equal(t, time.Second, c.OutboxWorker.Interval)
	require.Equal(t, 100, c.OutboxWorker.Limit)
}

func Test_New_Override(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequired(t)
	t.Setenv("HTTP_PORT", "9000")
	t.Setenv("SAGA_BANK_MAX_ATTEMPTS", "5")

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "9000", c.HTTP.Port)
	require.EqualValues(t, 5, c.Saga.BankMaxAttempts)
}

func Test_New_RequiresDependencies(t *testing.T) {
	t.Chdir(t.TempDir())
	setRequired(t)
	t.Setenv("LEDGER_ADDR", "")
	require.NoError(t, os.Unsetenv("LEDGER_ADDR"))

	_, err := config.New()
	require.ErrorContains(t, err, "LEDGER_ADDR")
}
