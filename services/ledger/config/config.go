package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"

	"github.com/vbloher/payment-processing-system/pkg/httpserver"
	"github.com/vbloher/payment-processing-system/pkg/logger"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/controller/grpc"
)

const (
	httpPortEnv     = "HTTP_PORT"
	defaultHTTPPort = "8081" // pkg/httpserver defaults to 8080, which is the payment service's port.
)

type Config struct {
	HTTP     httpserver.Config
	GRPC     grpc.Config
	Logger   logger.Config
	OTEL     otel.Config
	Postgres postgres.Config
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

	if _, ok := os.LookupEnv(httpPortEnv); !ok {
		config.HTTP.Port = defaultHTTPPort
	}

	return config, nil
}
