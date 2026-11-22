package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type TransactionKind string

const (
	KindDeposit TransactionKind = "deposit"
	KindPayment TransactionKind = "payment"
)

type Entry struct {
	AccountID uuid.UUID
	Side      Side
	Amount    int64
}

// Transaction is an immutable double-entry posting set.
type Transaction struct {
	ID                  uuid.UUID
	Kind                TransactionKind
	Reference, Currency string
	Entries             []Entry
	CreatedAt           time.Time
}

func NewTransaction(kind TransactionKind, reference, currency string, entries ...Entry) (Transaction, error) {
	if reference == "" {
		return Transaction{}, fmt.Errorf("domain.NewTransaction: empty reference: %w", ErrValidation)
	}

	if len(entries) < 2 {
		return Transaction{}, fmt.Errorf("domain.NewTransaction: need at least two entries, got %d: %w",
			len(entries), ErrValidation)
	}

	var debits, credits int64

	for _, e := range entries {
		err := ValidateMoney(e.Amount, currency)
		if err != nil {
			return Transaction{}, fmt.Errorf("domain.NewTransaction: %w", err)
		}

		switch e.Side {
		case Debit:
			debits += e.Amount
		case Credit:
			credits += e.Amount
		default:
			return Transaction{}, fmt.Errorf("domain.NewTransaction: side %q: %w", e.Side, ErrValidation)
		}
	}

	if debits != credits {
		return Transaction{}, fmt.Errorf("domain.NewTransaction: debits %d, credits %d: %w", debits, credits, ErrUnbalanced)
	}

	return Transaction{
		ID:        uuid.New(),
		Kind:      kind,
		Reference: reference,
		Currency:  currency,
		Entries:   append([]Entry(nil), entries...),
		CreatedAt: time.Now().UTC(),
	}, nil
}
