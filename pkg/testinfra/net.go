package testinfra

import (
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

const waitHTTPTimeout = 30 * time.Second

// FreePort returns a TCP port that was free at the time of the call.
func FreePort(t testing.TB) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testinfra.FreePort: %v", err)
	}
	defer func() { _ = l.Close() }()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("testinfra.FreePort: unexpected address type %T", l.Addr())
	}

	return strconv.Itoa(addr.Port)
}

// WaitHTTP polls url until it answers 200 and fails the test after 30s.
func WaitHTTP(t testing.TB, url string) {
	t.Helper()

	httpClient := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(waitHTTPTimeout)

	for {
		resp, err := httpClient.Get(url) //nolint:noctx // test helper polling a local endpoint
		if err == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("testinfra.WaitHTTP: %s not ready after %s", url, waitHTTPTimeout)
		}

		time.Sleep(100 * time.Millisecond)
	}
}
