package domain

import "errors"

var (
	ErrNotFound               = errors.New("not found")
	ErrValidation             = errors.New("validation failed")
	ErrIdempotencyKeyRequired = errors.New("idempotency key required")
	ErrIdempotencyConflict    = errors.New("idempotency key reused with a different request")
	ErrInvalidTransition      = errors.New("invalid status transition")
	ErrInsufficientFunds      = errors.New("insufficient funds")
	ErrAccountNotFound        = errors.New("account not found")
	ErrLedgerRejected         = errors.New("ledger rejected the request")
	ErrLedgerConflict         = errors.New("ledger hold is in a conflicting state")
	ErrBankUnavailable        = errors.New("bank outcome unknown")
)

// IsInvalidInput reports whether err is caused by invalid caller input.
func IsInvalidInput(err error) bool {
	return errors.Is(err, ErrValidation) || errors.Is(err, ErrIdempotencyKeyRequired)
}

// IsFatal reports whether retrying can never help, so a workflow must not retry the step.
func IsFatal(err error) bool {
	return errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrInvalidTransition) ||
		errors.Is(err, ErrValidation) ||
		errors.Is(err, ErrLedgerConflict)
}
