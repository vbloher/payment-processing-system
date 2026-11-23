package natsjs_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/natsjs"
)

// fakeServer accepts NATS clients on a local port. With handshake it speaks just enough of the
// protocol for a client to connect (INFO, then PONG to every PING); without it, it stays silent,
// so a client hangs in the handshake. stop closes the listener and every accepted connection.
func fakeServer(t *testing.T, handshake bool) (url string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		conns []net.Conn
	)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()

			go serve(conn, handshake)
		}
	}()

	stop = func() {
		_ = ln.Close()

		mu.Lock()
		defer mu.Unlock()

		for _, conn := range conns {
			_ = conn.Close()
		}
	}
	t.Cleanup(stop)

	return "nats://" + ln.Addr().String(), stop
}

func serve(conn net.Conn, handshake bool) {
	if !handshake {
		_, _ = io.Copy(io.Discard, conn)

		return
	}

	_, _ = conn.Write([]byte(`INFO {"server_id":"fake","version":"2.12.0","max_payload":1048576,"headers":true}` + "\r\n"))

	r := bufio.NewReader(conn)

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}

		if strings.HasPrefix(line, "PING") {
			_, _ = conn.Write([]byte("PONG\r\n"))
		}
	}
}

// The client never gives up reconnecting: the default closes the connection for good after 60
// attempts, which stalls the outbox relay and the consumer until a restart.
func Test_New_ReconnectsForever(t *testing.T) {
	url, _ := fakeServer(t, true)

	c, err := natsjs.New(t.Context(), natsjs.Config{URL: url})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	require.Equal(t, -1, c.Conn.Opts.MaxReconnect)
	require.Equal(t, 5*time.Second, c.Conn.Opts.Timeout)
	require.False(t, c.Conn.Opts.RetryOnFailedConnect, "a wrong URL must fail at startup")
}

func Test_New_FailsFastOnUnreachableURL(t *testing.T) {
	url, stop := fakeServer(t, true)
	stop()

	start := time.Now()

	_, err := natsjs.New(t.Context(), natsjs.Config{URL: url})
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
}

func Test_New_HonoursContext(t *testing.T) {
	url, _ := fakeServer(t, false)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()

	_, err := natsjs.New(ctx, natsjs.Config{URL: url})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
}

// Reconnecting forever must not keep Close from returning during an outage.
func Test_Close_DuringOutage(t *testing.T) {
	url, stop := fakeServer(t, true)

	c, err := natsjs.New(t.Context(), natsjs.Config{URL: url})
	require.NoError(t, err)

	stop()
	require.Eventually(t, c.Conn.IsReconnecting, 5*time.Second, 10*time.Millisecond)

	start := time.Now()

	c.Close()

	require.True(t, c.Conn.IsClosed())
	require.Less(t, time.Since(start), time.Second)
}
