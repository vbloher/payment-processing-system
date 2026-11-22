package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/ledger/config"
)

// setPostgres sets the required Postgres variables, which have no defaults.
func setPostgres(t *testing.T) {
	t.Helper()

	t.Setenv("POSTGRES_USER", "u")
	t.Setenv("POSTGRES_PASSWORD", "p")
	t.Setenv("POSTGRES_PORT", "5432")
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_DB_NAME", "ledger")
}

func Test_New_Defaults(t *testing.T) {
	// Run from an empty directory so no .env file is loaded.
	t.Chdir(t.TempDir())
	setPostgres(t)

	for _, name := range []string{"HTTP_PORT", "GRPC_PORT"} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "8081", c.HTTP.Port)
	require.Equal(t, "50051", c.GRPC.Port)
}

func Test_New_Override(t *testing.T) {
	t.Chdir(t.TempDir())
	setPostgres(t)
	t.Setenv("HTTP_PORT", "9000")

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "9000", c.HTTP.Port)
}
