package app

import (
	"context"
	"fmt"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"

	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/metrics"
	pgpool "github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/router"
	"github.com/vbloher/payment-processing-system/pkg/transaction"
	"github.com/vbloher/payment-processing-system/services/ledger/config"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/adapter/postgres"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/controller/grpc"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase"
)

// Run starts the service and returns nil once ctx is cancelled and shutdown completes.
// It touches no process-global state, so several services can run in one process.
func Run(ctx context.Context, c config.Config) error {
	pgPool, err := pgpool.New(ctx, c.Postgres)
	if err != nil {
		return fmt.Errorf("postgres.New: %w", err)
	}
	defer pgPool.Close()

	tx := transaction.New(pgPool)

	uc := usecase.New(tx, postgres.New(tx))

	grpcServer, err := grpc.New(ctx, c.GRPC, uc)
	if err != nil {
		return fmt.Errorf("grpc.New: %w", err)
	}
	defer grpcServer.Close()

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
