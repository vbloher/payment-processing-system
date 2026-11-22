package saga_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"

	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
)

type ReconciliationWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestReconciliationWorkflowSuite(t *testing.T) {
	suite.Run(t, new(ReconciliationWorkflowSuite))
}

func (s *ReconciliationWorkflowSuite) SetupTest() {
	s.SetLogger(log.NewStructuredLogger(slog.New(slog.DiscardHandler))) // the failures are expected
	s.env = s.NewTestWorkflowEnvironment()

	// Every call must go through an OnActivity expectation to be counted; the stub running means one did not.
	s.env.RegisterActivityWithOptions(func(context.Context) (saga.ReconcileResult, error) {
		s.T().Error("Reconcile ran without an OnActivity expectation")

		return saga.ReconcileResult{}, errBoom
	}, activity.RegisterOptions{Name: saga.ActivityReconcile})
}

func (s *ReconciliationWorkflowSuite) Test_ReconciliationWorkflow() {
	type call struct {
		result saga.ReconcileResult
		err    error
	}

	failed := call{err: errBoom}

	tests := []struct {
		name      string
		calls     []call // what each attempt returns, in order; the last one repeats
		want      saga.ReconcileResult
		wantErr   bool
		wantCalls int
	}{
		{name: "returns the run's result", calls: []call{{result: saga.ReconcileResult{Found: 2}}},
			want: saga.ReconcileResult{Found: 2}, wantCalls: 1},
		{name: "a run that fails twice is retried and succeeds",
			calls:     []call{failed, failed, {result: saga.ReconcileResult{Checked: 5, Resolved: 1}}},
			want:      saga.ReconcileResult{Checked: 5, Resolved: 1},
			wantCalls: 3},
		{name: "a run that always fails fails the workflow after three attempts", calls: []call{failed},
			wantErr: true, wantCalls: 3},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest() // fresh environment per row

			last := len(tt.calls) - 1
			for _, c := range tt.calls[:last] {
				s.env.OnActivity(saga.ActivityReconcile, mock.Anything).Return(c.result, c.err).Once()
			}

			s.env.OnActivity(saga.ActivityReconcile, mock.Anything).Return(tt.calls[last].result, tt.calls[last].err)

			s.env.ExecuteWorkflow(saga.ReconciliationWorkflow)

			require.True(s.T(), s.env.IsWorkflowCompleted())
			s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivityReconcile, tt.wantCalls)

			if tt.wantErr {
				// The activity's error crosses Temporal serialized: its message survives, its identity does not.
				require.ErrorContains(s.T(), s.env.GetWorkflowError(), errBoom.Error())

				return
			}

			require.NoError(s.T(), s.env.GetWorkflowError())

			var got saga.ReconcileResult

			require.NoError(s.T(), s.env.GetWorkflowResult(&got))
			require.Equal(s.T(), tt.want, got)
		})
	}
}
