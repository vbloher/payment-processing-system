package domain

import "errors"

var (
	ErrUnavailable = errors.New("bank unavailable")
	ErrTimeout     = errors.New("bank timeout")
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("idempotency key reused with a different request")
	ErrValidation  = errors.New("validation failed")
)
