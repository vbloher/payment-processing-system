//go:build integration

package testinfra_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/temporalclient"
	"github.com/vbloher/payment-processing-system/pkg/testinfra"
)

func TestStartPostgres_CreatesDatabases(t *testing.T) {
	pg := testinfra.StartPostgres(t, "one", "two")
	for _, name := range []string{"one", "two"} {
		conn, err := pgx.Connect(t.Context(), pg.URL(name))
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRow(t.Context(), "SELECT current_database()").Scan(&got))
		require.Equal(t, name, got)
		require.NoError(t, conn.Close(t.Context()))
	}
}

func TestStartNATS_JetStreamDeduplicates(t *testing.T) {
	c, err := natsjs.New(t.Context(), testinfra.StartNATS(t))
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.EnsureStreams(t.Context(), events.StreamConfig(), events.DLQStreamConfig()))

	first, err := c.JS.Publish(t.Context(), "events.payment.succeeded", []byte("x"), jetstream.WithMsgID("id-1"))
	require.NoError(t, err)
	require.False(t, first.Duplicate)
	second, err := c.JS.Publish(t.Context(), "events.payment.succeeded", []byte("x"), jetstream.WithMsgID("id-1"))
	require.NoError(t, err)
	require.True(t, second.Duplicate)
}

func TestStartTemporal_AcceptsConnections(t *testing.T) {
	c, err := temporalclient.New(t.Context(), testinfra.StartTemporal(t))
	require.NoError(t, err)
	defer c.Close()
	_, err = c.CheckHealth(t.Context(), &client.CheckHealthRequest{})
	require.NoError(t, err)
}

func TestFreePort_IsUsable(t *testing.T) {
	l, err := net.Listen("tcp", net.JoinHostPort("", testinfra.FreePort(t)))
	require.NoError(t, err)
	require.NoError(t, l.Close())
}

func TestMigrate_AppliesSQLMigrations(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1_init.up.sql"), []byte("CREATE TABLE t (id int);"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1_init.down.sql"), []byte("DROP TABLE t;"), 0o600))

	pg := testinfra.StartPostgres(t, "m")
	testinfra.Migrate(t, pg.URL("m"), dir)
	testinfra.Migrate(t, pg.URL("m"), dir) // no change is not an error

	conn, err := pgx.Connect(t.Context(), pg.URL("m"))
	require.NoError(t, err)
	defer func() { _ = conn.Close(t.Context()) }()
	var n int
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count(*) FROM t").Scan(&n))
}
