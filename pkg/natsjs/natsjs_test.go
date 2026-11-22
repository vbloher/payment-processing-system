//go:build integration

package natsjs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
)

// Close returns only once the drain has finished, so acknowledgements and publishes issued
// before it reach the server even when the process exits right after.
func Test_Close_WaitsForDrain(t *testing.T) {
	c, err := natsjs.New(t.Context(), testinfra.StartNATS(t))
	require.NoError(t, err)

	c.Close()

	require.True(t, c.Conn.IsClosed())
}
