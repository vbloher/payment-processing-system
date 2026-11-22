package saga

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

// PaymentInput configures one payment saga run.
type PaymentInput struct {
	PaymentID            uuid.UUID
	ActivityTimeout      time.Duration // StartToClose of every activity
	RetryInitialInterval time.Duration
	RetryMaxInterval     time.Duration
	BankMaxAttempts      int32
}

// FailPaymentInput is the input of the FailPayment activity.
type FailPaymentInput struct {
	PaymentID uuid.UUID
	Reason    domain.FailureReason
}

// PaymentWorkflow runs an outbound payment: hold funds (compensable), bank transfer (pivot),
// then capture and complete (forward-only, unlimited retries). Funds are released only on a
// definitive bank refusal; an unknown outcome goes through a bank cancel and, if still unknown,
// the payment is marked for review and the workflow waits for the bank-outcome signal.
func PaymentWorkflow(ctx workflow.Context, in PaymentInput) (domain.Status, error) {
	internal := withOptions(ctx, in, 0)
	bank := withOptions(ctx, in, in.BankMaxAttempts)
	id := in.PaymentID

	err := workflow.ExecuteActivity(internal, ActivityStartProcessing, id).Get(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("saga.PaymentWorkflow: start processing: %w", err)
	}

	var reason domain.FailureReason

	err = workflow.ExecuteActivity(internal, ActivityHoldFunds, id).Get(ctx, &reason)
	if err != nil {
		return "", fmt.Errorf("saga.PaymentWorkflow: hold funds: %w", err)
	}

	if reason != domain.ReasonNone {
		return failPayment(ctx, internal, id, reason)
	}

	outcome, err := resolveOutcome(ctx, internal, bank, id)
	if err != nil {
		return "", err
	}

	switch outcome {
	case domain.BankSucceeded:
		err := workflow.ExecuteActivity(internal, ActivityCaptureFunds, id).Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("saga.PaymentWorkflow: capture funds: %w", err)
		}

		err = workflow.ExecuteActivity(internal, ActivityCompletePayment, id).Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("saga.PaymentWorkflow: complete payment: %w", err)
		}

		return domain.StatusSucceeded, nil
	case domain.BankDeclined:
		return releaseAndFail(ctx, internal, id, domain.ReasonBankDeclined)
	case domain.BankCancelled:
		return releaseAndFail(ctx, internal, id, domain.ReasonBankCancelled)
	}

	// Never guess: an unknown value neither compensates nor completes.
	//nolint:wrapcheck // a workflow-level ApplicationError must reach Temporal unwrapped
	return "", temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("saga.PaymentWorkflow: unexpected bank outcome %q", outcome), "UnexpectedBankOutcome", nil)
}

// resolveOutcome returns the definitive bank outcome: from the transfer, from a cancel when
// the transfer outcome is unknown, or from the reconciliation signal when both are unknown.
func resolveOutcome(ctx, internal, bank workflow.Context, id uuid.UUID) (domain.BankOutcome, error) {
	var outcome domain.BankOutcome

	err := workflow.ExecuteActivity(bank, ActivitySendTransfer, id).Get(ctx, &outcome)
	if err == nil {
		return outcome, nil
	}

	if isFatal(err) {
		return "", fmt.Errorf("saga.PaymentWorkflow: send transfer: %w", err)
	}

	outcome = ""

	err = workflow.ExecuteActivity(bank, ActivityCancelTransfer, id).Get(ctx, &outcome)
	if err == nil {
		return outcome, nil
	}

	if isFatal(err) {
		return "", fmt.Errorf("saga.PaymentWorkflow: cancel transfer: %w", err)
	}

	err = workflow.ExecuteActivity(internal, ActivityMarkPaymentForReview, id).Get(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("saga.PaymentWorkflow: mark for review: %w", err)
	}

	outcome = ""
	workflow.GetSignalChannel(ctx, BankOutcomeSignal).Receive(ctx, &outcome)

	return outcome, nil
}

func releaseAndFail(ctx, internal workflow.Context, id uuid.UUID, reason domain.FailureReason) (domain.Status, error) {
	err := workflow.ExecuteActivity(internal, ActivityReleaseFunds, id).Get(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("saga.PaymentWorkflow: release funds: %w", err)
	}

	return failPayment(ctx, internal, id, reason)
}

func failPayment(ctx, internal workflow.Context, id uuid.UUID, reason domain.FailureReason) (domain.Status, error) {
	in := FailPaymentInput{PaymentID: id, Reason: reason}

	err := workflow.ExecuteActivity(internal, ActivityFailPayment, in).Get(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("saga.PaymentWorkflow: fail payment: %w", err)
	}

	return domain.StatusFailed, nil
}

func isFatal(err error) bool {
	var appErr *temporal.ApplicationError

	return errors.As(err, &appErr) && appErr.Type() == ErrTypeFatal
}

func withOptions(ctx workflow.Context, in PaymentInput, maxAttempts int32) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: in.ActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        in.RetryInitialInterval,
			BackoffCoefficient:     2,
			MaximumInterval:        in.RetryMaxInterval,
			MaximumAttempts:        maxAttempts,
			NonRetryableErrorTypes: []string{ErrTypeFatal},
		},
	})
}
