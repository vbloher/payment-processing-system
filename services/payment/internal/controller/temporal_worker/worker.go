package temporal_worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

var ErrInvalidConfig = errors.New("temporal worker: invalid reconciliation config")

type Config struct {
	ReconciliationInterval   time.Duration `env:"RECONCILIATION_INTERVAL"    envDefault:"1m"`
	ReconciliationLookback   time.Duration `env:"RECONCILIATION_LOOKBACK"    envDefault:"24h"`
	ReconciliationLag        time.Duration `env:"RECONCILIATION_LAG"         envDefault:"30s"`
	ReconciliationStuckAfter time.Duration `env:"RECONCILIATION_STUCK_AFTER" envDefault:"5m"`
}

// validate rejects values reconciliation cannot run with: a Lag of at least Lookback leaves an empty window.
func (c Config) validate() error {
	switch {
	case c.ReconciliationInterval <= 0:
		return fmt.Errorf("%w: interval must be positive, got %s", ErrInvalidConfig, c.ReconciliationInterval)
	case c.ReconciliationLookback <= 0:
		return fmt.Errorf("%w: lookback must be positive, got %s", ErrInvalidConfig, c.ReconciliationLookback)
	case c.ReconciliationLag < 0 || c.ReconciliationLag >= c.ReconciliationLookback:
		return fmt.Errorf("%w: lag must be in [0, lookback %s), got %s", ErrInvalidConfig,
			c.ReconciliationLookback, c.ReconciliationLag)
	case c.ReconciliationStuckAfter <= 0:
		return fmt.Errorf("%w: stuck threshold must be positive, got %s", ErrInvalidConfig,
			c.ReconciliationStuckAfter)
	}

	return nil
}

// Worker polls two task queues: the payment saga's and reconciliation's. Reconciliation runs on its own
// queue so that a long run never delays a payment's steps.
type Worker struct {
	payment        worker.Worker
	reconciliation worker.Worker
}

func New(ctx context.Context, c client.Client, cfg Config, uc *usecase.UseCase) (*Worker, error) {
	err := cfg.validate()
	if err != nil {
		return nil, err
	}

	activities := NewActivities(uc, cfg)

	w := &Worker{
		payment:        worker.New(c, saga.PaymentTaskQueue, worker.Options{}),
		reconciliation: worker.New(c, saga.ReconciliationTaskQueue, worker.Options{}),
	}

	w.payment.RegisterWorkflow(saga.PaymentWorkflow)
	activities.Register(w.payment)

	w.reconciliation.RegisterWorkflow(saga.ReconciliationWorkflow)
	activities.RegisterReconciliation(w.reconciliation)

	err = w.payment.Start()
	if err != nil {
		return nil, fmt.Errorf("payment worker.Start: %w", err)
	}

	err = w.reconciliation.Start()
	if err != nil {
		w.payment.Stop()

		return nil, fmt.Errorf("reconciliation worker.Start: %w", err)
	}

	err = createSchedule(ctx, c, cfg.ReconciliationInterval)
	if err != nil {
		w.Close()

		return nil, err
	}

	log.Info().Msg("temporal worker: started on task queues: " + saga.PaymentTaskQueue + ", " +
		saga.ReconciliationTaskQueue)

	return w, nil
}

// createSchedule makes Temporal start ReconciliationWorkflow every interval, skipping a start while the
// previous run is still going. The schedule, not a ticker in each replica, yields one run however many
// replicas there are. An existing schedule is kept as is: changing its interval means updating it in Temporal.
func createSchedule(ctx context.Context, c client.Client, interval time.Duration) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:   saga.ReconciliationScheduleID,
		Spec: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: interval}}},
		Action: &client.ScheduleWorkflowAction{
			ID:        "reconciliation",
			Workflow:  saga.ReconciliationWorkflow,
			TaskQueue: saga.ReconciliationTaskQueue,
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
	if err != nil && !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return fmt.Errorf("ScheduleClient.Create: %w", err)
	}

	return nil
}

func (w *Worker) Close() {
	w.reconciliation.Stop()
	w.payment.Stop()

	log.Info().Msg("temporal worker: closed")
}
