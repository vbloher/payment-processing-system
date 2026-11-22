package testinfra

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/vbloher/payment-processing-system/pkg/natsjs"
)

const (
	NATSImage = "nats:2.15.3"

	natsReadyTimeout = 30 * time.Second
)

// StartNATS starts a JetStream-enabled NATS server that ends with the test.
func StartNATS(t testing.TB) natsjs.Config {
	t.Helper()

	ctr, err := tcnats.Run(t.Context(), NATSImage, testcontainers.WithCmd("-js"),
		// The module's port wait passes as soon as the host-side proxy listens, before the server does.
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForListeningPort("4222/tcp"),
			wait.ForLog("Server is ready"),
		)),
	)
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("testinfra.StartNATS: %v", err)
	}

	url, err := ctr.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("testinfra.StartNATS: connection string: %v", err)
	}

	waitNATSReady(t, url)

	return natsjs.Config{URL: url}
}

// waitNATSReady retries until a client connects and JetStream answers.
func waitNATSReady(t testing.TB, url string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), natsReadyTimeout)
	defer cancel()

	var lastErr error

	for ctx.Err() == nil {
		lastErr = probeJetStream(ctx, url)
		if lastErr == nil {
			return
		}

		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}

	t.Fatalf("testinfra.StartNATS: not ready after %s: %v", natsReadyTimeout, lastErr)
}

func probeJetStream(ctx context.Context, url string) error {
	nc, err := nats.Connect(url)
	if err != nil {
		return err //nolint:wrapcheck // retried and reported by the caller
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err //nolint:wrapcheck // retried and reported by the caller
	}

	_, err = js.AccountInfo(ctx)

	return err //nolint:wrapcheck // retried and reported by the caller
}
