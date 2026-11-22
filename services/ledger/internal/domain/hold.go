package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type HoldStatus string

const (
	HoldActive   HoldStatus = "active"
	HoldCaptured HoldStatus = "captured"
	HoldReleased HoldStatus = "released"
)

// Hold is a reservation of funds on an account for one payment.
//
//nolint:recvcheck // Available/SameRequest are read-only value methods; mutators need a pointer.
type Hold struct {
	PaymentID, AccountID uuid.UUID
	Amount               int64
	Currency             string
	Status               HoldStatus
	CreatedAt, UpdatedAt time.Time
}

func NewHold(paymentID, accountID uuid.UUID, amount int64, currency string) (Hold, error) {
	err := ValidateMoney(amount, currency)
	if err != nil {
		return Hold{}, fmt.Errorf("domain.NewHold: %w", err)
	}

	now := time.Now().UTC()

	return Hold{
		PaymentID: paymentID,
		AccountID: accountID,
		Amount:    amount,
		Currency:  currency,
		Status:    HoldActive,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// SameRequest reports whether a repeated reserve call carries the same parameters.
func (h Hold) SameRequest(accountID uuid.UUID, amount int64, currency string) bool {
	return h.AccountID == accountID && h.Amount == amount && h.Currency == currency
}

// Capture moves an active hold to captured. Repeating it is a no-op.
func (h *Hold) Capture() (changed bool, err error) {
	return h.transition(HoldCaptured, HoldReleased)
}

// Release moves an active hold to released. Repeating it is a no-op.
func (h *Hold) Release() (changed bool, err error) {
	return h.transition(HoldReleased, HoldCaptured)
}

func (h *Hold) transition(to, conflicting HoldStatus) (bool, error) {
	if h.Status == to {
		return false, nil
	}

	if h.Status == conflicting {
		return false, fmt.Errorf("domain.Hold: %s hold cannot become %s: %w", h.Status, to, ErrHoldConflict)
	}

	h.Status = to
	h.UpdatedAt = time.Now().UTC()

	return true, nil
}
