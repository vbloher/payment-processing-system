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
	"github.com/vbloher/payment-processing-system/pkg/natsjs"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/pkg/postgres"
	"github.com/vbloher/payment-processing-system/services/notification/internal/adapter/webhook"
	"github.com/vbloher/payment-processing-system/services/notification/internal/controller/nats_consumer"
)

const (
	httpPortEnv     = "HTTP_PORT"
	defaultHTTPPort = "8082" // pkg/httpserver defaults to 8080, which is the payment service's port.
)

var ErrInvalidConfig = errors.New("config: invalid")

type Config struct {
	HTTP     httpserver.Config
	Logger   logger.Config
	OTEL     otel.Config
	Postgres postgres.Config
	NATS     natsjs.Config
	Consumer nats_consumer.Config
	Webhook  webhook.Config
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

// Validate checks the settings that depend on each other. A webhook call must time out before
// the consumer's handler deadline: otherwise a slow receiver uses up the handler's whole time on
// every delivery, and a timeout of 0 would leave the call bounded by that deadline alone.
func (c Config) Validate() error {
	handlerTimeout := c.Consumer.HandlerTimeout()

	if c.Webhook.Timeout <= 0 || c.Webhook.Timeout >= handlerTimeout {
		return fmt.Errorf("%w: webhook timeout %s must be positive and below the consumer's handler timeout %s",
			ErrInvalidConfig, c.Webhook.Timeout, handlerTimeout)
	}

	return nil
}
