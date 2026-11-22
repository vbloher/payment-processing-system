package temporalclient

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

type Config struct {
	HostPort  string `env:"TEMPORAL_HOST_PORT,required"`
	Namespace string `env:"TEMPORAL_NAMESPACE"          envDefault:"default"`
}

func New(ctx context.Context, c Config) (client.Client, error) {
	cl, err := client.DialContext(ctx, client.Options{
		HostPort:  c.HostPort,
		Namespace: c.Namespace,
		Logger:    newLogger(),
	})
	if err != nil {
		return nil, fmt.Errorf("client.DialContext: %w", err)
	}

	return cl, nil
}
