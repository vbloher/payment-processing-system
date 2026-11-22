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
	"github.com/vbloher/payment-processing-system/pkg/temporalclient"
	"github.com/vbloher/payment-processing-system/pkg/transaction"
	"github.com/vbloher/payment-processing-system/services/payment/config"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/bank"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/ledger"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/nats_publisher"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/orchestrator"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/postgres"
	controller "github.com/vbloher/payment-processing-system/services/payment/internal/controller/http"
	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/outbox_worker"
	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/temporal_worker"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

// Run starts the service and returns nil once ctx is cancelled and shutdown completes.
// It touches no process-global state, so several services can run in one process.
// Components close in reverse order: controllers first, then adapters.
//
//nolint:funlen,cyclop // a linear sequence of constructors; every branch is an early error return
func Run(ctx context.Context, c config.Config) error {
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

	err = natsClient.EnsureStreams(ctx, events.StreamConfig(), events.DLQStreamConfig())
	if err != nil {
		return fmt.Errorf("natsClient.EnsureStreams: %w", err)
	}

	temporalClient, err := temporalclient.New(ctx, c.Temporal)
	if err != nil {
		return fmt.Errorf("temporalclient.New: %w", err)
	}
	defer temporalClient.Close()

	ledgerClient, err := ledger.New(c.Ledger)
	if err != nil {
		return fmt.Errorf("ledger.New: %w", err)
	}
	defer ledgerClient.Close()

	bankClient, err := bank.New(c.Bank)
	if err != nil {
		return fmt.Errorf("bank.New: %w", err)
	}

	reg := prometheus.NewRegistry()

	publisher := nats_publisher.New(natsClient, metrics.NewProcess(reg))

	orch, err := orchestrator.New(temporalClient, c.Saga)
	if err != nil {
		return fmt.Errorf("orchestrator.New: %w", err)
	}

	uc := usecase.New(tx, postgres.New(tx), ledgerClient, bankClient, publisher, orch)

	temporalWorker, err := temporal_worker.New(temporalClient, c.TemporalWorker, uc)
	if err != nil {
		return fmt.Errorf("temporal_worker.New: %w", err)
	}
	defer temporalWorker.Close()

	outboxWorker, err := outbox_worker.New(c.OutboxWorker, uc)
	if err != nil {
		return fmt.Errorf("outbox_worker.New: %w", err)
	}
	defer outboxWorker.Close()

	r := router.New()
	httpMetrics := metrics.NewHTTPServer(reg)

	r.Group(func(r chi.Router) {
		r.Use(metrics.NewMiddleware(httpMetrics))
		r.Handle("/metrics", metrics.Handler(reg))
	})

	err = controller.PaymentRouter(r, uc, httpMetrics)
	if err != nil {
		return fmt.Errorf("controller.PaymentRouter: %w", err)
	}

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
