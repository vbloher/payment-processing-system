package testinfra

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"

	"github.com/vbloher/payment-processing-system/pkg/temporalclient"
)

const (
	TemporalImage = "temporalio/temporal:1.10.1"

	temporalPort          = "7233/tcp"
	temporalHealthTimeout = 30 * time.Second
)

// StartTemporal starts a Temporal dev server that ends with the test.
func StartTemporal(t testing.TB) temporalclient.Config {
	t.Helper()

	ctr, err := testcontainers.Run(t.Context(), TemporalImage,
		testcontainers.WithCmd("server", "start-dev", "--ip", "0.0.0.0"),
		testcontainers.WithExposedPorts(temporalPort),
		testcontainers.WithWaitStrategy(wait.ForListeningPort(temporalPort)),
	)
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("testinfra.StartTemporal: %v", err)
	}

	hostPort, err := ctr.PortEndpoint(t.Context(), temporalPort, "")
	if err != nil {
		t.Fatalf("testinfra.StartTemporal: endpoint: %v", err)
	}

	cfg := temporalclient.Config{HostPort: hostPort, Namespace: "default"}
	waitTemporalHealthy(t, cfg)

	return cfg
}

// waitTemporalHealthy retries CheckHealth: the port opens before the frontend serves.
func waitTemporalHealthy(t testing.TB, cfg temporalclient.Config) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), temporalHealthTimeout)
	defer cancel()

	var lastErr error

	for ctx.Err() == nil {
		c, err := temporalclient.New(ctx, cfg)
		if err == nil {
			_, err = c.CheckHealth(ctx, &client.CheckHealthRequest{})
			c.Close()

			if err == nil {
				return
			}
		}

		lastErr = err

		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}

	t.Fatalf("testinfra.StartTemporal: not healthy after %s: %v", temporalHealthTimeout, lastErr)
}
