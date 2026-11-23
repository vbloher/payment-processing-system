package natsjs

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

const (
	// connectTimeout bounds each dial and handshake, at startup and on every reconnect attempt.
	connectTimeout = 5 * time.Second
	// closeTimeout bounds how long Close waits for the drain to finish.
	closeTimeout = 10 * time.Second
)

type Config struct {
	URL string `env:"NATS_URL,required"`
}

type Client struct {
	Conn   *nats.Conn
	JS     jetstream.JetStream
	closed chan struct{} // closed once the connection has closed
}

// New connects to NATS, giving up when ctx ends first. Once connected, the client reconnects for
// as long as the broker is down: the library's default gives up after 60 attempts and closes the
// connection for good, which would stall the outbox relay and the consumer until a restart. The
// first connect is not retried, so a wrong URL fails at startup.
func New(ctx context.Context, c Config) (*Client, error) {
	closed := make(chan struct{})

	conn, err := connect(ctx, c.URL,
		nats.Timeout(connectTimeout),
		nats.MaxReconnects(-1),
		nats.ClosedHandler(func(*nats.Conn) { close(closed) }),
	)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	return &Client{Conn: conn, JS: js, closed: closed}, nil
}

// connect runs nats.Connect, which takes no context, and returns ctx's error when ctx ends first;
// a connection that is established after that is closed.
func connect(ctx context.Context, url string, opts ...nats.Option) (*nats.Conn, error) {
	type result struct {
		conn *nats.Conn
		err  error
	}

	done := make(chan result, 1)

	go func() {
		conn, err := nats.Connect(url, opts...)
		done <- result{conn: conn, err: err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			return nil, fmt.Errorf("nats.Connect: %w", r.err)
		}

		return r.conn, nil
	case <-ctx.Done():
		go func() {
			if r := <-done; r.conn != nil {
				r.conn.Close()
			}
		}()

		return nil, fmt.Errorf("nats.Connect: %w", ctx.Err())
	}
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
