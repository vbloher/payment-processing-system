package domain

import (
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusSucceeded Status = "succeeded"
	StatusDeclined  Status = "declined"
	StatusCancelled Status = "cancelled"
)

// Beneficiary prefixes select a deterministic failure mode.
const (
	PrefixDecline  = "DECLINE"  // declined, reason DeclineReasonAccountClosed
	PrefixFlaky    = "FLAKY"    // the first FlakyFailures create attempts fail unprocessed, then success
	PrefixTimeout  = "TIMEOUT"  // processed, but every create call answers with a timeout
	PrefixBlackout = "BLACKOUT" // processed, then the bank turns itself unavailable

	FlakyFailures              = 2
	DeclineReasonAccountClosed = "account_closed"
)

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

type Transfer struct {
	ID                    uuid.UUID
	Reference             string // the client's Idempotency-Key
	Amount                int64
	Currency, Beneficiary string
	Status                Status
	DeclineReason         string
	CreatedAt             time.Time
}

// ValidateRequest checks the fields of a create request.
func ValidateRequest(reference string, amount int64, currency, beneficiary string) error {
	if reference == "" || amount < 1 || !currencyRe.MatchString(currency) || beneficiary == "" {
		return ErrValidation
	}

	return nil
}

// HasPrefix reports whether the beneficiary selects the failure mode.
func (t Transfer) HasPrefix(prefix string) bool {
	return strings.HasPrefix(t.Beneficiary, prefix)
}

// SameRequest reports whether the transfer was created from the same request body.
func (t Transfer) SameRequest(amount int64, currency, beneficiary string) bool {
	return t.Amount == amount && t.Currency == currency && t.Beneficiary == beneficiary
}
