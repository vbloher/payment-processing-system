package outbox_worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

var ErrInvalidConfig = errors.New("outbox worker: interval and limit must be positive")

type Config struct {
	Interval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"1s"`
	Limit    int           `env:"OUTBOX_LIMIT"    envDefault:"100"`
}

type Worker struct {
	config  Config
	usecase *usecase.UseCase
	stop    context.CancelFunc
	done    chan struct{}
}

func New(c Config, uc *usecase.UseCase) (*Worker, error) {
	if c.Interval <= 0 || c.Limit <= 0 {
		return nil, fmt.Errorf("%w: %+v", ErrInvalidConfig, c)
	}

	ctx, stop := context.WithCancel(context.Background())

	w := &Worker{
		config:  c,
		usecase: uc,
		stop:    stop,
		done:    make(chan struct{}),
	}

	go w.run(tracer.Suppress(ctx))

	return w, nil
}

func (w *Worker) run(ctx context.Context) {
	defer close(w.done)

	log.Info().Msg("outbox worker: started")

	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.send(ctx)
		}
	}
}

func (w *Worker) send(ctx context.Context) {
	for ctx.Err() == nil {
		sent, err := w.usecase.SendOutbox(ctx, w.config.Limit)
		if err != nil {
			log.Error().Err(err).Msg("outbox worker: usecase.SendOutbox")

			return
		}

		if sent < w.config.Limit {
			return
		}
	}
}

func (w *Worker) Close() {
	log.Info().Msg("outbox worker: closing")

	w.stop()

	<-w.done

	log.Info().Msg("outbox worker: closed")
}
