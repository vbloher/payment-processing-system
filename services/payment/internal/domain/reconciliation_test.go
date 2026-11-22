package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func TestReconcile(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

	bank := func(status domain.BankOutcome, amount int64, currency string) *domain.BankTransfer {
		return &domain.BankTransfer{ID: "b1", Reference: "ref", Amount: amount, Currency: currency, Status: status}
	}

	const (
		missing    = domain.KindBankMissing
		unexpected = domain.KindBankUnexpected
		amountMis  = domain.KindAmountMismatch
		ledgerMis  = domain.KindLedgerMismatch
		stuck      = domain.KindStuck
	)

	tests := []struct {
		name   string
		status domain.Status
		ago    time.Duration
		bank   *domain.BankTransfer
		hold   domain.HoldState
		want   []domain.DiscrepancyKind
	}{
		{"1 consistent success", domain.StatusSucceeded, time.Hour, bank(domain.BankSucceeded, 3000, "EUR"), domain.HoldCaptured, nil},
		{"2 succeeded, no transfer", domain.StatusSucceeded, time.Hour, nil, domain.HoldCaptured, []domain.DiscrepancyKind{missing}},
		{"3 succeeded, bank declined", domain.StatusSucceeded, time.Hour, bank(domain.BankDeclined, 3000, "EUR"), domain.HoldCaptured, []domain.DiscrepancyKind{missing}},
		{"4 succeeded, bank cancelled", domain.StatusSucceeded, time.Hour, bank(domain.BankCancelled, 3000, "EUR"), domain.HoldCaptured, []domain.DiscrepancyKind{missing}},
		{"5 amount differs", domain.StatusSucceeded, time.Hour, bank(domain.BankSucceeded, 2999, "EUR"), domain.HoldCaptured, []domain.DiscrepancyKind{amountMis}},
		{"6 currency differs", domain.StatusSucceeded, time.Hour, bank(domain.BankSucceeded, 3000, "USD"), domain.HoldCaptured, []domain.DiscrepancyKind{amountMis}},
		{"7 hold still active", domain.StatusSucceeded, time.Hour, bank(domain.BankSucceeded, 3000, "EUR"), domain.HoldActive, []domain.DiscrepancyKind{ledgerMis}},
		{"8 hold released", domain.StatusSucceeded, time.Hour, bank(domain.BankSucceeded, 3000, "EUR"), domain.HoldReleased, []domain.DiscrepancyKind{ledgerMis}},
		{"9 no transfer, no hold", domain.StatusSucceeded, time.Hour, nil, domain.HoldAbsent, []domain.DiscrepancyKind{missing, ledgerMis}},
		{"10 failed, nothing anywhere", domain.StatusFailed, time.Hour, nil, domain.HoldAbsent, nil},
		{"11 failed, cancelled, released", domain.StatusFailed, time.Hour, bank(domain.BankCancelled, 3000, "EUR"), domain.HoldReleased, nil},
		{"12 failed, declined, released", domain.StatusFailed, time.Hour, bank(domain.BankDeclined, 3000, "EUR"), domain.HoldReleased, nil},
		{"13 failed, bank succeeded", domain.StatusFailed, time.Hour, bank(domain.BankSucceeded, 3000, "EUR"), domain.HoldReleased, []domain.DiscrepancyKind{unexpected}},
		{"14 failed, hold active", domain.StatusFailed, time.Hour, nil, domain.HoldActive, []domain.DiscrepancyKind{ledgerMis}},
		{"15 failed, hold captured", domain.StatusFailed, time.Hour, nil, domain.HoldCaptured, []domain.DiscrepancyKind{ledgerMis}},
		{"16 processing, fresh", domain.StatusProcessing, 10 * time.Second, nil, domain.HoldActive, nil},
		{"17 processing, stale", domain.StatusProcessing, 10 * time.Minute, bank(domain.BankSucceeded, 3000, "EUR"), domain.HoldActive, []domain.DiscrepancyKind{stuck}},
		{"18 review, stale", domain.StatusReview, 10 * time.Minute, nil, domain.HoldActive, []domain.DiscrepancyKind{stuck}},
		{"19 created, stale", domain.StatusCreated, 10 * time.Minute, nil, domain.HoldAbsent, []domain.DiscrepancyKind{stuck}},
		{"20 created, fresh", domain.StatusCreated, 10 * time.Second, nil, domain.HoldAbsent, nil},
		{"exactly at threshold is not stuck", domain.StatusProcessing, 5 * time.Minute, nil, domain.HoldActive, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.Reconcile(domain.ReconcileFacts{
				Payment: domain.Payment{
					ID: uuid.New(), Amount: 3000, Currency: "EUR",
					Status: tt.status, UpdatedAt: now.Add(-tt.ago),
				},
				Bank:       tt.bank,
				Hold:       tt.hold,
				Now:        now,
				StuckAfter: 5 * time.Minute,
			})
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDiscrepancy_ToEvent(t *testing.T) {
	d := domain.NewDiscrepancy(domain.KindBankMissing, "ref-1", uuid.New(), "no transfer")
	require.NotEqual(t, uuid.Nil, d.ID)
	require.Equal(t, domain.DiscrepancyOpen, d.Status)
	require.False(t, d.CreatedAt.IsZero())

	ev, err := d.ToEvent()
	require.NoError(t, err)
	require.Equal(t, "events.reconciliation.discrepancy_found", ev.Subject)

	env, err := events.Unmarshal(ev.Payload)
	require.NoError(t, err)
	require.Equal(t, d.PaymentID, env.PaymentID)

	var data events.DiscrepancyData
	require.NoError(t, json.Unmarshal(env.Data, &data))
	require.Equal(t, events.DiscrepancyData{
		DiscrepancyID: d.ID, Kind: "bank_missing", Reference: d.Reference, Details: d.Details,
	}, data)
}
