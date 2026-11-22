package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	MaxAmount int64 = 1_000_000_000_000

	maxKeyLen         = 255
	maxBeneficiaryLen = 140
	maxDescriptionLen = 255
)

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// Payment is an outbound payment. TransitionTo mutates it, so it has a pointer receiver;
// the read-only methods keep value receivers as the contract requires.
//
//nolint:recvcheck // receiver mix is part of the task contract
type Payment struct {
	ID             uuid.UUID
	IdempotencyKey string
	RequestHash    string
	AccountID      uuid.UUID
	Amount         int64
	Currency       string
	Beneficiary    string
	Description    string
	Status         Status
	FailureReason  FailureReason
	BankReference  string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewPayment validates the input and returns a payment in StatusCreated.
// Lengths are counted in characters (Unicode code points), as the HTTP contract counts them.
func NewPayment(
	idempotencyKey string, accountID uuid.UUID, amount int64, currency, beneficiary, description string,
) (Payment, error) {
	if idempotencyKey == "" {
		return Payment{}, fmt.Errorf("domain.NewPayment: %w", ErrIdempotencyKeyRequired)
	}

	switch {
	case utf8.RuneCountInString(idempotencyKey) > maxKeyLen:
		return Payment{}, fmt.Errorf("domain.NewPayment: %w: idempotency key longer than %d", ErrValidation, maxKeyLen)
	case accountID == uuid.Nil:
		return Payment{}, fmt.Errorf("domain.NewPayment: %w: account id is required", ErrValidation)
	case amount < 1 || amount > MaxAmount:
		return Payment{}, fmt.Errorf("domain.NewPayment: %w: amount must be in 1..%d", ErrValidation, MaxAmount)
	case !currencyRe.MatchString(currency):
		return Payment{}, fmt.Errorf("domain.NewPayment: %w: currency must match ^[A-Z]{3}$", ErrValidation)
	case beneficiary == "" || utf8.RuneCountInString(beneficiary) > maxBeneficiaryLen:
		return Payment{}, fmt.Errorf(
			"domain.NewPayment: %w: beneficiary must be 1..%d characters", ErrValidation, maxBeneficiaryLen,
		)
	case utf8.RuneCountInString(description) > maxDescriptionLen:
		return Payment{}, fmt.Errorf("domain.NewPayment: %w: description longer than %d", ErrValidation, maxDescriptionLen)
	}

	now := time.Now().UTC()

	return Payment{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		RequestHash:    RequestHash(accountID, amount, currency, beneficiary, description),
		AccountID:      accountID,
		Amount:         amount,
		Currency:       currency,
		Beneficiary:    beneficiary,
		Description:    description,
		Status:         StatusCreated,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// RequestHash returns the hex SHA-256 of the request fields. Strings are length-prefixed,
// so different field splits ("ab"+"c" vs "a"+"bc") cannot collide.
func RequestHash(accountID uuid.UUID, amount int64, currency, beneficiary, description string) string {
	h := sha256.New()

	h.Write(accountID[:])
	_ = binary.Write(h, binary.BigEndian, amount)

	for _, s := range []string{currency, beneficiary, description} {
		_ = binary.Write(h, binary.BigEndian, uint64(len(s)))
		h.Write([]byte(s))
	}

	return hex.EncodeToString(h.Sum(nil))
}
