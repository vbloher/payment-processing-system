package orchestrator_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/payment/internal/adapter/orchestrator"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
)

func validConfig() orchestrator.Config {
	return orchestrator.Config{
		ActivityTimeout:      10 * time.Second,
		RetryInitialInterval: time.Second,
		RetryMaxInterval:     30 * time.Second,
		BankMaxAttempts:      3,
	}
}

func Test_New_RejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *orchestrator.Config)
	}{
		{"zero bank attempts would retry the bank forever", func(c *orchestrator.Config) { c.BankMaxAttempts = 0 }},
		{"negative bank attempts", func(c *orchestrator.Config) { c.BankMaxAttempts = -1 }},
		{"zero activity timeout", func(c *orchestrator.Config) { c.ActivityTimeout = 0 }},
		{"zero initial retry interval", func(c *orchestrator.Config) { c.RetryInitialInterval = 0 }},
		{"max retry interval below the initial one", func(c *orchestrator.Config) {
			c.RetryMaxInterval = c.RetryInitialInterval - time.Millisecond
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)

			o, err := orchestrator.New(mocks.NewClient(t), c)
			require.ErrorIs(t, err, orchestrator.ErrInvalidConfig)
			require.Nil(t, o)
		})
	}

	t.Run("one bank attempt is valid", func(t *testing.T) {
		c := validConfig()
		c.BankMaxAttempts = 1

		_, err := orchestrator.New(mocks.NewClient(t), c)
		require.NoError(t, err)
	})
}

func Test_StartPayment(t *testing.T) {
	otel.SilentModeInit()

	errDown := errors.New("down")

	tests := []struct {
		name      string
		clientErr error
		wantErr   error
	}{
		{"started", nil, nil},
		{"a running or finished run exists", serviceerror.NewWorkflowExecutionAlreadyStarted("exists", "", ""), nil},
		{"temporal unavailable", errDown, errDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := mocks.NewClient(t)
			cfg := validConfig()
			id := uuid.New()

			wantOptions := mock.MatchedBy(func(o client.StartWorkflowOptions) bool {
				return o.ID == saga.PaymentWorkflowID(id) &&
					o.TaskQueue == saga.PaymentTaskQueue &&
					o.WorkflowIDReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
			})
			wantInput := saga.PaymentInput{
				PaymentID:            id,
				ActivityTimeout:      cfg.ActivityTimeout,
				RetryInitialInterval: cfg.RetryInitialInterval,
				RetryMaxInterval:     cfg.RetryMaxInterval,
				BankMaxAttempts:      cfg.BankMaxAttempts,
			}

			c.On("ExecuteWorkflow", mock.Anything, wantOptions, mock.Anything, wantInput).
				Return(nil, tt.clientErr).Once()

			o, err := orchestrator.New(c, cfg)
			require.NoError(t, err)

			err = o.StartPayment(t.Context(), id)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_SignalBankOutcome(t *testing.T) {
	otel.SilentModeInit()

	errDown := errors.New("down")

	for _, clientErr := range []error{nil, errDown} {
		c := mocks.NewClient(t)
		id := uuid.New()

		c.On("SignalWorkflow", mock.Anything, saga.PaymentWorkflowID(id), "", saga.BankOutcomeSignal, domain.BankCancelled).
			Return(clientErr).Once()

		o, err := orchestrator.New(c, validConfig())
		require.NoError(t, err)

		err = o.SignalBankOutcome(t.Context(), id, domain.BankCancelled)
		if clientErr != nil {
			require.ErrorIs(t, err, clientErr)
		} else {
			require.NoError(t, err)
		}
	}
}
