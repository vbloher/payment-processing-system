package config

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"

	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/logger"
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/pkg/temporalclient"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/bank"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/ledger"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/orchestrator"
	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/outbox_worker"
	"github.com/vbloher/payment-processing-system/services/payment/internal/controller/temporal_worker"
)

// Config of the payment service. Its HTTP port defaults to 8080, pkg/httpserver's default.
type Config struct {
	HTTP           httpserver.Config
	Logger         logger.Config
	OTEL           otel.Config
	Postgres       postgres.Config
	NATS           natsjs.Config
	Temporal       temporalclient.Config
	Ledger         ledger.Config
	Bank           bank.Config
	Saga           orchestrator.Config
	TemporalWorker temporal_worker.Config
	OutboxWorker   outbox_worker.Config
}

func New() (Config, error) {
	var config Config

	err := godotenv.Load(".env")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return config, fmt.Errorf("godotenv.Load: %w", err)
	}

	err = env.Parse(&config)
	if err != nil {
		return config, fmt.Errorf("env.Parse: %w", err)
	}

	return config, nil
}
