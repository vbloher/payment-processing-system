package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
)

var ErrInvalidConfig = errors.New("orchestrator: invalid saga config")

type Config struct {
	ActivityTimeout      time.Duration `env:"SAGA_ACTIVITY_TIMEOUT"       envDefault:"10s"`
	RetryInitialInterval time.Duration `env:"SAGA_RETRY_INITIAL_INTERVAL" envDefault:"1s"`
	RetryMaxInterval     time.Duration `env:"SAGA_RETRY_MAX_INTERVAL"     envDefault:"30s"`
	BankMaxAttempts      int32         `env:"SAGA_BANK_MAX_ATTEMPTS"      envDefault:"3"`
}

// validate rejects values the saga cannot run with. A BankMaxAttempts of 0 means unlimited
// attempts in Temporal: the bank would be retried forever and the cancel/review path never run.
func (c Config) validate() error {
	switch {
	case c.ActivityTimeout <= 0:
		return fmt.Errorf("%w: activity timeout must be positive, got %s", ErrInvalidConfig, c.ActivityTimeout)
	case c.RetryInitialInterval <= 0:
		return fmt.Errorf("%w: initial retry interval must be positive, got %s", ErrInvalidConfig,
			c.RetryInitialInterval)
	case c.RetryMaxInterval < c.RetryInitialInterval:
		return fmt.Errorf("%w: max retry interval %s is below the initial one %s", ErrInvalidConfig,
			c.RetryMaxInterval, c.RetryInitialInterval)
	case c.BankMaxAttempts < 1:
		return fmt.Errorf("%w: bank max attempts must be at least 1, got %d", ErrInvalidConfig, c.BankMaxAttempts)
	}

	return nil
}

// Orchestrator starts and signals payment workflows in Temporal.
type Orchestrator struct {
	client client.Client
	config Config
}

func New(c client.Client, cfg Config) (*Orchestrator, error) {
	err := cfg.validate()
	if err != nil {
		return nil, err
	}

	return &Orchestrator{client: c, config: cfg}, nil
}

// StartPayment starts the payment's workflow. The workflow ID is derived from the payment ID and
// reuse is rejected, so each payment runs at most one saga: a running or finished run is not an error.
func (o *Orchestrator) StartPayment(ctx context.Context, paymentID uuid.UUID) error {
	ctx, span := tracer.Start(ctx, "adapter temporal StartPayment")
	defer span.End()

	_, err := o.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    saga.PaymentWorkflowID(paymentID),
		TaskQueue:             saga.PaymentTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		// Surface "already started" instead of the SDK's silent default, so the rule is explicit here.
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, saga.PaymentWorkflow, saga.PaymentInput{
		PaymentID:            paymentID,
		ActivityTimeout:      o.config.ActivityTimeout,
		RetryInitialInterval: o.config.RetryInitialInterval,
		RetryMaxInterval:     o.config.RetryMaxInterval,
		BankMaxAttempts:      o.config.BankMaxAttempts,
	})
	if err != nil {
		if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
			return nil
		}

		return fmt.Errorf("client.ExecuteWorkflow: %w", err)
	}

	return nil
}

// SignalBankOutcome delivers the definitive bank outcome to the payment's running workflow.
func (o *Orchestrator) SignalBankOutcome(ctx context.Context, paymentID uuid.UUID, outcome domain.BankOutcome) error {
	ctx, span := tracer.Start(ctx, "adapter temporal SignalBankOutcome")
	defer span.End()

	err := o.client.SignalWorkflow(ctx, saga.PaymentWorkflowID(paymentID), "", saga.BankOutcomeSignal, outcome)
	if err != nil {
		return fmt.Errorf("client.SignalWorkflow: %w", err)
	}

	return nil
}
