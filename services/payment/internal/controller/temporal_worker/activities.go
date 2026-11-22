package temporal_worker

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

// Activities are the saga steps. Each one calls one use case and classifies its error.
type Activities struct {
	usecase *usecase.UseCase
}

func NewActivities(uc *usecase.UseCase) *Activities {
	return &Activities{usecase: uc}
}

// Register registers every activity under its saga.Activity* name, the name the workflow calls.
func (a *Activities) Register(r worker.ActivityRegistry) {
	for name, fn := range map[string]any{
		saga.ActivityStartProcessing:      a.StartProcessing,
		saga.ActivityHoldFunds:            a.HoldFunds,
		saga.ActivitySendTransfer:         a.SendTransfer,
		saga.ActivityCancelTransfer:       a.CancelTransfer,
		saga.ActivityCaptureFunds:         a.CaptureFunds,
		saga.ActivityReleaseFunds:         a.ReleaseFunds,
		saga.ActivityCompletePayment:      a.CompletePayment,
		saga.ActivityFailPayment:          a.FailPayment,
		saga.ActivityMarkPaymentForReview: a.MarkPaymentForReview,
	} {
		r.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
}

func (a *Activities) StartProcessing(ctx context.Context, paymentID uuid.UUID) error {
	return classify(a.usecase.StartProcessing(ctx, paymentID))
}

func (a *Activities) HoldFunds(ctx context.Context, paymentID uuid.UUID) (domain.FailureReason, error) {
	reason, err := a.usecase.HoldFunds(ctx, paymentID)

	return reason, classify(err)
}

func (a *Activities) SendTransfer(ctx context.Context, paymentID uuid.UUID) (domain.BankOutcome, error) {
	outcome, err := a.usecase.SendTransfer(ctx, paymentID)

	return outcome, classify(err)
}

func (a *Activities) CancelTransfer(ctx context.Context, paymentID uuid.UUID) (domain.BankOutcome, error) {
	outcome, err := a.usecase.CancelTransfer(ctx, paymentID)

	return outcome, classify(err)
}

func (a *Activities) CaptureFunds(ctx context.Context, paymentID uuid.UUID) error {
	return classify(a.usecase.CaptureFunds(ctx, paymentID))
}

func (a *Activities) ReleaseFunds(ctx context.Context, paymentID uuid.UUID) error {
	return classify(a.usecase.ReleaseFunds(ctx, paymentID))
}

func (a *Activities) CompletePayment(ctx context.Context, paymentID uuid.UUID) error {
	return classify(a.usecase.CompletePayment(ctx, paymentID))
}

func (a *Activities) FailPayment(ctx context.Context, in saga.FailPaymentInput) error {
	return classify(a.usecase.FailPayment(ctx, in.PaymentID, in.Reason))
}

func (a *Activities) MarkPaymentForReview(ctx context.Context, paymentID uuid.UUID) error {
	return classify(a.usecase.MarkPaymentForReview(ctx, paymentID))
}

// classify turns an error that retrying can never fix into a non-retryable Fatal failure.
// Any other error, an unknown bank outcome included, is returned as is for Temporal to retry.
func classify(err error) error {
	if err == nil || !domain.IsFatal(err) {
		return err
	}

	//nolint:wrapcheck // the ApplicationError must reach Temporal as is; err stays its cause
	return temporal.NewNonRetryableApplicationError(err.Error(), saga.ErrTypeFatal, err)
}
