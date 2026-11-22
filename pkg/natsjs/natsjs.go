package natsjs

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

// closeTimeout bounds how long Close waits for the drain to finish.
const closeTimeout = 10 * time.Second

type Config struct {
	URL string `env:"NATS_URL,required"`
}

type Client struct {
	Conn   *nats.Conn
	JS     jetstream.JetStream
	closed chan struct{} // closed once the connection has closed
}

func New(_ context.Context, c Config) (*Client, error) {
	closed := make(chan struct{})

	conn, err := nats.Connect(c.URL, nats.ClosedHandler(func(*nats.Conn) { close(closed) }))
	if err != nil {
		return nil, fmt.Errorf("nats.Connect: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	return &Client{Conn: conn, JS: js, closed: closed}, nil
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

// Close drains the connection and returns once it has closed, so that pending messages,
// acknowledgements included, are flushed before the process exits. The wait is bounded.
func (c *Client) Close() {
	err := c.Conn.Drain()
	if err != nil {
		c.Conn.Close()
	}

	select {
	case <-c.closed:
	case <-time.After(closeTimeout):
		log.Warn().Msg("nats: drain did not finish in time, closing")
		c.Conn.Close()
	}

	log.Info().Msg("nats: closed")
}
