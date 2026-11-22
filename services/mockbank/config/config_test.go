package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/services/mockbank/config"
)

func Test_New_Defaults(t *testing.T) {
	// Run from an empty directory so no .env file is loaded.
	t.Chdir(t.TempDir())
	t.Setenv("HTTP_PORT", "")
	require.NoError(t, os.Unsetenv("HTTP_PORT"))

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "8090", c.HTTP.Port)
}

func Test_New_Override(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("HTTP_PORT", "9000")

	c, err := config.New()
	require.NoError(t, err)
	require.Equal(t, "9000", c.HTTP.Port)
}
