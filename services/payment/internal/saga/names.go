// Package saga holds the Temporal workflow definitions. It is the only business code that
// imports the Temporal SDK; activities are referenced by name so the package never imports
// the controller that implements them.
package saga

import "github.com/google/uuid"

const (
	// PaymentTaskQueue is the task queue the payment worker polls.
	PaymentTaskQueue = "payment"
	// ReconciliationTaskQueue is the task queue the reconciliation worker polls.
	ReconciliationTaskQueue = "reconciliation"
	// ReconciliationScheduleID is the ID of the Temporal Schedule that starts ReconciliationWorkflow.
	ReconciliationScheduleID = "reconciliation"
	// BankOutcomeSignal carries a domain.BankOutcome from reconciliation to a waiting workflow.
	BankOutcomeSignal = "bank-outcome"
	// ErrTypeFatal is the ApplicationError type of a non-retryable activity failure.
	ErrTypeFatal = "Fatal"
)

// Activity names. The controller registers each activity under exactly this name.
const (
	ActivityStartProcessing      = "StartProcessing"      // (uuid.UUID) error
	ActivityHoldFunds            = "HoldFunds"            // (uuid.UUID) (domain.FailureReason, error)
	ActivitySendTransfer         = "SendTransfer"         // (uuid.UUID) (domain.BankOutcome, error)
	ActivityCancelTransfer       = "CancelTransfer"       // (uuid.UUID) (domain.BankOutcome, error)
	ActivityCaptureFunds         = "CaptureFunds"         // (uuid.UUID) error
	ActivityReleaseFunds         = "ReleaseFunds"         // (uuid.UUID) error
	ActivityCompletePayment      = "CompletePayment"      // (uuid.UUID) error
	ActivityFailPayment          = "FailPayment"          // (FailPaymentInput) error
	ActivityMarkPaymentForReview = "MarkPaymentForReview" // (uuid.UUID) error
	ActivityReconcile            = "Reconcile"            // () (ReconcileResult, error)
)

// PaymentWorkflowID is the workflow ID of a payment's saga.
func PaymentWorkflowID(paymentID uuid.UUID) string {
	return "payment-" + paymentID.String()
}
