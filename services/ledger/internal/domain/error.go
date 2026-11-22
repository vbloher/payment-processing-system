package domain

import (
	"errors"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrValidation          = errors.New("validation failed")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrHoldConflict        = errors.New("hold is in a conflicting state")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different parameters")
	ErrUnbalanced          = errors.New("debits and credits are not equal")
)

func IsInvalidInput(err error) bool {
	return errors.Is(err, ErrValidation) ||
		errors.Is(err, ErrCurrencyMismatch) ||
		errors.Is(err, ErrUnbalanced)
}
