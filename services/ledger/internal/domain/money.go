package domain

import (
	"fmt"
	"regexp"
)

// MaxAmount is the largest amount, in minor units, accepted by the ledger.
const MaxAmount int64 = 1_000_000_000_000

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// ValidateMoney checks that amount is within 1..MaxAmount and currency looks like an ISO-4217 code.
func ValidateMoney(amount int64, currency string) error {
	if amount < 1 || amount > MaxAmount {
		return fmt.Errorf("domain.ValidateMoney: amount %d out of range: %w", amount, ErrValidation)
	}

	return validateCurrency(currency)
}

func validateCurrency(currency string) error {
	if !currencyRe.MatchString(currency) {
		return fmt.Errorf("domain.validateCurrency: %q: %w", currency, ErrValidation)
	}

	return nil
}
