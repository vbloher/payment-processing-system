package natsjs

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

type Config struct {
	URL string `env:"NATS_URL,required"`
}

type Client struct {
	Conn *nats.Conn
	JS   jetstream.JetStream
}

func New(_ context.Context, c Config) (*Client, error) {
	conn, err := nats.Connect(c.URL)
	if err != nil {
		return nil, fmt.Errorf("nats.Connect: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	return &Client{Conn: conn, JS: js}, nil
}

// EnsureStreams creates each stream or updates it to match the configuration.
func (c *Client) EnsureStreams(ctx context.Context, configs ...jetstream.StreamConfig) error {
	for _, cfg := range configs {
		_, err := c.JS.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("js.CreateOrUpdateStream %s: %w", cfg.Name, err)
		}
	}

	return nil
}

// Close drains the connection: pending messages are flushed before it closes.
func (c *Client) Close() {
	err := c.Conn.Drain()
	if err != nil {
		c.Conn.Close()
	}

	log.Info().Msg("nats: closed")
}
