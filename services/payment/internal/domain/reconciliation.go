package domain

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
)

// DiscrepancyKind classifies a disagreement between a payment, the bank and the ledger.
type DiscrepancyKind string

const (
	KindBankMissing    DiscrepancyKind = "bank_missing"
	KindBankUnexpected DiscrepancyKind = "bank_unexpected"
	KindAmountMismatch DiscrepancyKind = "amount_mismatch"
	KindLedgerMismatch DiscrepancyKind = "ledger_mismatch"
	KindStuck          DiscrepancyKind = "stuck"
)

// DiscrepancyStatus is the lifecycle state of a recorded discrepancy.
type DiscrepancyStatus string

const (
	DiscrepancyOpen     DiscrepancyStatus = "open"
	DiscrepancyResolved DiscrepancyStatus = "resolved"
)

// Discrepancy is a recorded disagreement found by reconciliation.
type Discrepancy struct {
	ID         uuid.UUID
	Kind       DiscrepancyKind
	Reference  string    // payment ID as a string, or the bank reference when no payment exists
	PaymentID  uuid.UUID // uuid.Nil when no payment exists
	Details    string
	Status     DiscrepancyStatus
	CreatedAt  time.Time
	ResolvedAt time.Time
}

// NewDiscrepancy returns an open discrepancy with a fresh ID.
func NewDiscrepancy(kind DiscrepancyKind, reference string, paymentID uuid.UUID, details string) Discrepancy {
	return Discrepancy{
		ID:        uuid.New(),
		Kind:      kind,
		Reference: reference,
		PaymentID: paymentID,
		Details:   details,
		Status:    DiscrepancyOpen,
		CreatedAt: time.Now().UTC(),
	}
}

// ToEvent builds the outbox event announcing the discrepancy.
func (d Discrepancy) ToEvent() (Event, error) {
	t := events.TypeDiscrepancyFound

	env, err := events.New(t, d.PaymentID, events.DiscrepancyData{
		DiscrepancyID: d.ID,
		Kind:          string(d.Kind),
		Reference:     d.Reference,
		Details:       d.Details,
	})
	if err != nil {
		return Event{}, fmt.Errorf("domain.Discrepancy.ToEvent: %w", err)
	}

	payload, err := env.Marshal()
	if err != nil {
		return Event{}, fmt.Errorf("domain.Discrepancy.ToEvent: %w", err)
	}

	return Event{ID: env.ID, Subject: t.Subject(), Payload: payload, Headers: map[string]string{}}, nil
}

// ReconcileFacts is everything Reconcile needs to know about one payment.
type ReconcileFacts struct {
	Payment    Payment
	Bank       *BankTransfer // nil when the bank has no transfer for the payment
	Hold       HoldState
	Now        time.Time
	StuckAfter time.Duration
}

// Reconcile compares one payment with the bank's and the ledger's view of it.
//
// Rules:
//   - A non-terminal payment yields only stuck, and only when Now - UpdatedAt > StuckAfter
//     (strictly): bank and ledger are legitimately mid-flight.
//   - Succeeded: the bank transfer must be succeeded (absent, declined or cancelled is
//     bank_missing); when it succeeded, amount and currency must equal the payment's
//     (else amount_mismatch); the hold must be captured (else ledger_mismatch).
//   - Failed: a succeeded bank transfer is bank_unexpected; the hold must be absent or
//     released (else ledger_mismatch). Amount is not compared for a failed payment: the
//     money must not have moved at all, so bank_unexpected already covers the case.
//
// The result is sorted and has no duplicates; nil means the payment is consistent.
func Reconcile(f ReconcileFacts) []DiscrepancyKind {
	var kinds []DiscrepancyKind

	switch f.Payment.Status {
	case StatusSucceeded:
		kinds = reconcileSucceeded(f)
	case StatusFailed:
		kinds = reconcileFailed(f)
	case StatusCreated, StatusProcessing, StatusReview:
		if f.Now.Sub(f.Payment.UpdatedAt) > f.StuckAfter {
			kinds = append(kinds, KindStuck)
		}
	}

	slices.Sort(kinds)

	return slices.Compact(kinds)
}

func reconcileSucceeded(f ReconcileFacts) []DiscrepancyKind {
	var kinds []DiscrepancyKind

	switch {
	case f.Bank == nil || f.Bank.Status != BankSucceeded:
		kinds = append(kinds, KindBankMissing)
	case f.Bank.Amount != f.Payment.Amount || f.Bank.Currency != f.Payment.Currency:
		kinds = append(kinds, KindAmountMismatch)
	}

	if f.Hold != HoldCaptured {
		kinds = append(kinds, KindLedgerMismatch)
	}

	return kinds
}

func reconcileFailed(f ReconcileFacts) []DiscrepancyKind {
	var kinds []DiscrepancyKind

	if f.Bank != nil && f.Bank.Status == BankSucceeded {
		kinds = append(kinds, KindBankUnexpected)
	}

	if f.Hold != HoldAbsent && f.Hold != HoldReleased {
		kinds = append(kinds, KindLedgerMismatch)
	}

	return kinds
}
