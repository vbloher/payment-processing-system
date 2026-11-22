package app

import (
	"context"
	"fmt"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"

	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/metrics"
	"github.com/vbloher/payment-processing-system/pkg/router"
	"github.com/vbloher/payment-processing-system/services/mockbank/config"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/adapter/memory"
	controller "github.com/vbloher/payment-processing-system/services/mockbank/internal/controller/http"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/usecase"
)

// Run starts the service and returns nil once ctx is cancelled and shutdown completes.
// It touches no process-global state, so several services can run in one process.
func Run(ctx context.Context, c config.Config) error {
	uc := usecase.New(memory.New())

	reg := prometheus.NewRegistry()

	r := router.New()
	httpMetrics := metrics.NewHTTPServer(reg)

	r.Group(func(r chi.Router) {
		r.Use(metrics.NewMiddleware(httpMetrics))
		r.Handle("/metrics", metrics.Handler(reg))
	})

	err := controller.BankRouter(r, uc, httpMetrics)
	if err != nil {
		return fmt.Errorf("controller.BankRouter: %w", err)
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
