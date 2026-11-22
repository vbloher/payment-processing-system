package saga

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ReconcileResult counts what one reconciliation run did.
type ReconcileResult struct {
	Checked, Found, Resolved, Signaled, Restarted int
}

// ReconciliationWorkflow runs one reconciliation as a single activity. A failed run is retried a few
// times and then fails: the schedule starts the next one, so nothing is retried forever.
func ReconciliationWorkflow(ctx workflow.Context) (ReconcileResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	var result ReconcileResult

	err := workflow.ExecuteActivity(ctx, ActivityReconcile).Get(ctx, &result)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("saga.ReconciliationWorkflow: reconcile: %w", err)
	}

	return result, nil
}
