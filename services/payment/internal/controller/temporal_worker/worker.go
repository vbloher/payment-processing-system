package temporal_worker

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

type Config struct{} // fields arrive with the reconciliation worker

// Worker polls the payment task queue and runs the payment workflow and its activities.
type Worker struct {
	worker worker.Worker
}

func New(c client.Client, _ Config, uc *usecase.UseCase) (*Worker, error) {
	w := worker.New(c, saga.PaymentTaskQueue, worker.Options{})

	w.RegisterWorkflow(saga.PaymentWorkflow)
	NewActivities(uc).Register(w)

	err := w.Start()
	if err != nil {
		return nil, fmt.Errorf("worker.Start: %w", err)
	}

	log.Info().Msg("temporal worker: started on task queue: " + saga.PaymentTaskQueue)

	return &Worker{worker: w}, nil
}

func (w *Worker) Close() {
	w.worker.Stop()

	log.Info().Msg("temporal worker: closed")
}
