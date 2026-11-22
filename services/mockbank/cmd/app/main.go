package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/vbloher/payment-processing-system/pkg/logger"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/mockbank/config"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/app"
)

func main() {
	err := run()
	if err != nil {
		log.Fatal().Err(err).Msg("app")
	}
}

func run() error {
	c, err := config.New()
	if err != nil {
		return fmt.Errorf("config.New: %w", err)
	}

	logger.Init(c.Logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = otel.Init(ctx, c.OTEL)
	if err != nil {
		return fmt.Errorf("otel.Init: %w", err)
	}
	defer otel.Close()

	err = app.Run(ctx, c)
	if err != nil {
		return fmt.Errorf("app.Run: %w", err)
	}

	return nil
}
