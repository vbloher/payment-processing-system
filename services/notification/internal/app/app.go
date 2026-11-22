package app

import (
	"context"
	"fmt"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/metrics"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	pgpool "github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/router"
	"github.com/vbloher/payment-processing-system/pkg/transaction"
	"github.com/vbloher/payment-processing-system/services/notification/config"
	"github.com/vbloher/payment-processing-system/services/notification/internal/adapter/postgres"
	"github.com/vbloher/payment-processing-system/services/notification/internal/adapter/webhook"
	"github.com/vbloher/payment-processing-system/services/notification/internal/controller/nats_consumer"
	"github.com/vbloher/payment-processing-system/services/notification/internal/usecase"
)

// Run starts the service and returns nil once ctx is cancelled and shutdown completes.
// It touches no process-global state, so several services can run in one process.
// Components close in reverse order: controllers first, then adapters.
func Run(ctx context.Context, c config.Config) error {
	// Checked here rather than in config.New, so that configs built by hand are checked too.
	err := c.Validate()
	if err != nil {
		return fmt.Errorf("config.Validate: %w", err)
	}

	pgPool, err := pgpool.New(ctx, c.Postgres)
	if err != nil {
		return fmt.Errorf("postgres.New: %w", err)
	}
	defer pgPool.Close()

	tx := transaction.New(pgPool)

	natsClient, err := natsjs.New(ctx, c.NATS)
	if err != nil {
		return fmt.Errorf("natsjs.New: %w", err)
	}
	defer natsClient.Close()

	// The consumer reads EVENTS and dead-letters into EVENTS_DLQ: both exist before it starts.
	err = natsClient.EnsureStreams(ctx, events.StreamConfig(), events.DLQStreamConfig())
	if err != nil {
		return fmt.Errorf("natsClient.EnsureStreams: %w", err)
	}

	uc := usecase.New(tx, postgres.New(tx), webhook.New(c.Webhook))

	consumer, err := nats_consumer.New(ctx, c.Consumer, natsClient, uc)
	if err != nil {
		return fmt.Errorf("nats_consumer.New: %w", err)
	}
	defer consumer.Close()

	reg := prometheus.NewRegistry()

	r := router.New()
	httpMetrics := metrics.NewHTTPServer(reg)

	r.Group(func(r chi.Router) {
		r.Use(metrics.NewMiddleware(httpMetrics))
		r.Handle("/metrics", metrics.Handler(reg))
	})

	httpServer, err := httpserver.New(ctx, r, c.HTTP)
	if err != nil {
		return fmt.Errorf("httpserver.New: %w", err)
	}
	defer httpServer.Close()

	log.Info().Msg("app: started")

	<-ctx.Done()

	log.Info().Msg("app: got signal to stop")

	return nil
}
