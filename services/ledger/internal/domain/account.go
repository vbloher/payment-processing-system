package domain

import (
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

type Side string

const (
	Debit  Side = "debit"
	Credit Side = "credit"
)

type AccountKind string

const (
	KindCustomer AccountKind = "customer"
	KindBankCash AccountKind = "bank_cash"
)

// Account is a ledger account. Balance is signed in the account's normal side:
// a customer account is a credit-normal liability, bank_cash a debit-normal asset.
// Held is a reservation outside postings; Available = Balance - Held.
//
//nolint:recvcheck // Available is a read-only value method; mutators need a pointer.
type Account struct {
	ID                   uuid.UUID
	Kind                 AccountKind
	Currency             string
	NormalSide           Side
	Balance, Held        int64
	CreatedAt, UpdatedAt time.Time
}

func NewCustomerAccount(currency string) (Account, error) {
	return newAccount(KindCustomer, Credit, currency)
}

func NewBankCashAccount(currency string) (Account, error) {
	return newAccount(KindBankCash, Debit, currency)
}

func newAccount(kind AccountKind, normal Side, currency string) (Account, error) {
	err := validateCurrency(currency)
	if err != nil {
		return Account{}, fmt.Errorf("domain.newAccount: %w", err)
	}

	now := time.Now().UTC()

	return Account{
		ID:         uuid.New(),
		Kind:       kind,
		Currency:   currency,
		NormalSide: normal,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

func (a Account) Available() int64 {
	return a.Balance - a.Held
}

// Post applies one entry to the balance. On error the account is unchanged.
func (a *Account) Post(side Side, amount int64, currency string) error {
	err := ValidateMoney(amount, currency)
	if err != nil {
		return fmt.Errorf("domain.Account.Post: %w", err)
	}

	if currency != a.Currency {
		return fmt.Errorf("domain.Account.Post: %s vs %s: %w", currency, a.Currency, ErrCurrencyMismatch)
	}

	delta := amount
	if side != a.NormalSide {
		delta = -amount
	}

	if (delta > 0 && a.Balance > math.MaxInt64-delta) || (delta < 0 && a.Balance < math.MinInt64-delta) {
		return fmt.Errorf("domain.Account.Post: balance overflow: %w", ErrValidation)
	}

	next := a.Balance + delta
	if a.Kind == KindCustomer && next < a.Held {
		return fmt.Errorf("domain.Account.Post: balance %d, held %d, delta %d: %w",
			a.Balance, a.Held, delta, ErrInsufficientFunds)
	}

	a.Balance = next
	a.UpdatedAt = time.Now().UTC()

	return nil
}

// Reserve holds amount out of the available balance. On error the account is unchanged.
func (a *Account) Reserve(amount int64, currency string) error {
	err := ValidateMoney(amount, currency)
	if err != nil {
		return fmt.Errorf("domain.Account.Reserve: %w", err)
	}

	if currency != a.Currency {
		return fmt.Errorf("domain.Account.Reserve: %s vs %s: %w", currency, a.Currency, ErrCurrencyMismatch)
	}

	if a.Available() < amount {
		return fmt.Errorf("domain.Account.Reserve: available %d, need %d: %w", a.Available(), amount, ErrInsufficientFunds)
	}

	a.Held += amount
	a.UpdatedAt = time.Now().UTC()

	return nil
}

// Unreserve returns amount to the available balance.
func (a *Account) Unreserve(amount int64) {
	a.Held -= amount
	a.UpdatedAt = time.Now().UTC()
}
