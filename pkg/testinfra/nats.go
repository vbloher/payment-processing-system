package testinfra

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/vbloher/payment-processing-system/pkg/natsjs"
)

const NATSImage = "nats:2.15.3"

// StartNATS starts a JetStream-enabled NATS server that ends with the test.
func StartNATS(t testing.TB) natsjs.Config {
	t.Helper()

	ctr, err := tcnats.Run(t.Context(), NATSImage, testcontainers.WithCmd("-js"))
	testcontainers.CleanupContainer(t, ctr)

	if err != nil {
		t.Fatalf("testinfra.StartNATS: %v", err)
	}

	url, err := ctr.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("testinfra.StartNATS: connection string: %v", err)
	}

	return natsjs.Config{URL: url}
}
