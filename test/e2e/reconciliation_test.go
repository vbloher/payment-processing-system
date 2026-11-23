//go:build integration

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/vbloher/payment-processing-system/pkg/events"

	bank_client "github.com/vbloher/payment-processing-system/gen/mockbank/http/client"
	payment_client "github.com/vbloher/payment-processing-system/gen/payment/http/client"
	enumspb "go.temporal.io/api/enums/v1"
)

// Spec scenarios 6 and 7: the bank's outcome stays unknown, so the payment waits in review with its funds
// held; once the bank is back, reconciliation cancels the transfer and signals the bank's final answer.
func (s *Suite) Test_Review_ResolvedByReconciliation() {
	tests := []struct {
		name        string
		bankDown    bool // the bank is down before the payment reaches it
		beneficiary string
		wantStatus  string
		wantReason  string
		wantBalance int64
		wantBank    string
	}{
		{name: "bank down before processing", bankDown: true, beneficiary: "DE-OK-1",
			wantStatus: "failed", wantReason: "bank_cancelled", wantBalance: 10000, wantBank: "cancelled"},
		{name: "bank processed, then went dark", beneficiary: "BLACKOUT-1",
			wantStatus: "succeeded", wantBalance: 7000, wantBank: "succeeded"},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.restoreBankOnCleanup() // BLACKOUT turns the bank off by itself

			account := s.openAccount("EUR", 10000)

			if tt.bankDown {
				s.setBankAvailable(false)
			}

			id, code := s.createPayment(uuid.NewString(), account, amount, "EUR", tt.beneficiary)
			s.Equal(http.StatusAccepted, code)

			s.waitStatus(id, "review")

			_, held, _ := s.balance(account)
			s.EqualValues(amount, held, "held in review")
			s.Equal(events.TypePaymentReviewRequired, s.waitWebhookCall(id).Envelope.Type)

			s.setBankAvailable(true)
			s.reconcile()

			p := s.waitStatus(id, tt.wantStatus)
			s.Equal(tt.wantReason, p.FailureReason)
			s.requireBalance(account, tt.wantBalance, 0)
			s.Equal(tt.wantBank, s.bankTransferStatus(id.String()), "bank transfer")
			s.requireLedgerIntact()

			s.reconcile()

			p = s.getPayment(id)
			s.Equal(tt.wantStatus, string(p.Status), "status after a second reconciliation")
			s.Equal(tt.wantReason, p.FailureReason)
			s.requireBalance(account, tt.wantBalance, 0)
		})
	}
}

// Spec scenario 13: a transfer the payment service never made and a transfer whose amount drifted are each
// recorded once however many runs see them, announced once, and resolved once the drift is gone.
func (s *Suite) Test_Reconciliation_FindsBankDiscrepancies() {
	ctx := context.Background()

	rogue, err := s.bank.AddTransferWithResponse(ctx, bank_client.AddTransferJSONRequestBody{
		Reference: "ROGUE-1", Amount: 500, Currency: "EUR", Beneficiary: "X",
	})
	s.NoError(err)
	s.Equal(http.StatusCreated, rogue.StatusCode(), string(rogue.Body))

	s.reconcile()
	s.reconcile()

	s.Equal([]string{"bank_unexpected"}, s.openDiscrepancies("ROGUE-1"))
	s.Equal(1, s.discrepancyRows("ROGUE-1"))

	account := s.openAccount("EUR", 10000)

	id, code := s.createPayment(uuid.NewString(), account, amount, "EUR", "DE-OK-1")
	s.Equal(http.StatusAccepted, code)
	s.waitStatus(id, "succeeded")
	s.setTransferAmount(id.String(), 1)

	s.reconcile()
	s.reconcile()

	s.Equal([]string{"amount_mismatch"}, s.openDiscrepancies(id.String()))
	s.Equal(1, s.discrepancyRows(id.String()))

	unexpected, mismatch := s.openDiscrepanciesOf("ROGUE-1")[0], s.openDiscrepanciesOf(id.String())[0]
	s.Nil(unexpected.PaymentID, "a bank reference that matches no payment")
	s.Equal(&id, mismatch.PaymentID)

	s.requireOneEventPerDiscrepancy(unexpected, mismatch)

	s.setTransferAmount(id.String(), amount)
	s.reconcile()

	s.Empty(s.openDiscrepancies(id.String()))

	var status string

	s.NoError(s.paymentDB.QueryRow(ctx,
		"SELECT status FROM reconciliation_discrepancy WHERE id = $1", mismatch.ID).Scan(&status))
	s.Equal("resolved", status)

	var resolvedAt *time.Time

	for _, d := range s.discrepancies(payment_client.DiscrepancyStatusResolved) {
		if d.ID == mismatch.ID {
			resolvedAt = d.ResolvedAt
		}
	}

	s.NotNil(resolvedAt, "the API lists the discrepancy as resolved")
}

// requireOneEventPerDiscrepancy waits for the reconciliation.discrepancy_found webhook call of each
// discrepancy, then checks that no second one follows.
func (s *Suite) requireOneEventPerDiscrepancy(ds ...payment_client.Discrepancy) {
	for _, d := range ds {
		var calls []webhookCall

		s.Eventually(func() bool {
			calls = s.discrepancyEvents(d.ID)

			return len(calls) > 0
		}, waitTimeout, waitTick, "no event for discrepancy %s", d.ID)

		var data events.DiscrepancyData

		s.NoError(json.Unmarshal(calls[0].Envelope.Data, &data))
		s.Equal(string(d.Kind), data.Kind)
		s.Equal(d.Reference, data.Reference)
		s.Equal(calls[0].Envelope.ID.String(), calls[0].IdempotencyKey)
	}

	s.Never(func() bool {
		for _, d := range ds {
			if len(s.discrepancyEvents(d.ID)) > 1 {
				return true
			}
		}

		return false
	}, time.Second, waitTick, "a second event for a discrepancy")
}

// Spec scenario 14: the payment row was written but its workflow never started; reconciliation starts it.
func (s *Suite) Test_Reconciliation_RestartsStuckCreated() {
	const beneficiary = "DE-OK-9"

	account := s.openAccount("EUR", 10000)
	id, key := uuid.New(), uuid.NewString()

	_, err := s.paymentDB.Exec(context.Background(), `
INSERT INTO payment (id, idempotency_key, request_hash, account_id, amount, currency, beneficiary, status,
                     created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'EUR', $6, 'created', now() - interval '1 minute', now() - interval '1 minute')`,
		id, key, requestHash(account, amount, "EUR", beneficiary, ""), account, amount, beneficiary)
	s.NoError(err)

	s.reconcile()

	s.waitStatus(id, "succeeded")
	s.requireBalance(account, 10000-amount, 0)

	// The row is the one the API would have written: replaying its request finds the same payment.
	replayed, code := s.createPayment(key, account, amount, "EUR", beneficiary)
	s.Equal(http.StatusAccepted, code)
	s.Equal(id, replayed)
}

// Spec section 8: the service schedules reconciliation in Temporal with overlapping runs skipped, and the
// scheduled action names a workflow the service runs: triggering the schedule runs it to completion.
func (s *Suite) Test_Reconciliation_Scheduled() {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	handle := s.temporal.ScheduleClient().GetHandle(ctx, "reconciliation")

	desc, err := handle.Describe(ctx)
	s.NoError(err)
	s.Equal([]client.ScheduleIntervalSpec{{Every: time.Hour}}, desc.Schedule.Spec.Intervals)
	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, desc.Schedule.Policy.Overlap)

	action, ok := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	s.True(ok, "a workflow action")
	s.Equal("ReconciliationWorkflow", action.Workflow)
	s.Equal("reconciliation", action.TaskQueue)

	first := s.triggerSchedule(ctx, handle)
	second := s.triggerSchedule(ctx, handle)
	s.NotEqual(first.FirstExecutionRunID, second.FirstExecutionRunID, "each trigger is checked on the run it started")
}

// triggerSchedule triggers the schedule and waits for the run it started to complete. RecentActions is sorted
// oldest first and capped, so the triggered run is the one that replaces the last element seen before.
func (s *Suite) triggerSchedule(ctx context.Context, handle client.ScheduleHandle) *client.ScheduleWorkflowExecution {
	desc, err := handle.Describe(ctx)
	s.Require().NoError(err)

	previous := lastScheduledRun(desc)

	// Allow overlap for this one trigger: the scheduler may not have seen the previous run finish yet, and the
	// schedule's skip policy would then drop the trigger.
	s.Require().NoError(handle.Trigger(ctx,
		client.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))

	var started *client.ScheduleWorkflowExecution

	s.Require().Eventually(func() bool {
		desc, err := handle.Describe(ctx)
		if err != nil {
			return false
		}

		started = lastScheduledRun(desc)

		// Runs triggered within one second share a workflow ID; the run ID tells them apart.
		return started != nil && (previous == nil || started.FirstExecutionRunID != previous.FirstExecutionRunID)
	}, waitTimeout, waitTick, "the triggered schedule started no workflow")

	s.Require().NoError(s.temporal.GetWorkflow(ctx, started.WorkflowID, started.FirstExecutionRunID).Get(ctx, nil))

	return started
}

// lastScheduledRun returns the newest run the schedule started, or nil.
func lastScheduledRun(desc *client.ScheduleDescription) *client.ScheduleWorkflowExecution {
	actions := desc.Info.RecentActions
	if len(actions) == 0 {
		return nil
	}

	return actions[len(actions)-1].StartWorkflowResult
}

// A payment the bank and the ledger agree with yields no discrepancy.
func (s *Suite) Test_Reconciliation_CleanRunFindsNothing() {
	account := s.openAccount("EUR", 10000)

	id, code := s.createPayment(uuid.NewString(), account, amount, "EUR", "DE-OK-1")
	s.Equal(http.StatusAccepted, code)
	s.waitStatus(id, "succeeded")

	s.reconcile()

	s.Empty(s.openDiscrepancies(id.String()))
}

// requireBalance checks the account's ledger balance and held amount.
func (s *Suite) requireBalance(account uuid.UUID, wantBalance, wantHeld int64) {
	balance, held, _ := s.balance(account)
	s.Equal(wantBalance, balance, "balance")
	s.Equal(wantHeld, held, "held")
}

// requestHash mirrors the payment service's domain.RequestHash, which test/e2e cannot import: SHA-256 over the
// account ID, the big-endian amount and the length-prefixed strings.
func requestHash(accountID uuid.UUID, amount int64, currency, beneficiary, description string) string {
	h := sha256.New()

	h.Write(accountID[:])
	_ = binary.Write(h, binary.BigEndian, amount)

	for _, str := range []string{currency, beneficiary, description} {
		_ = binary.Write(h, binary.BigEndian, uint64(len(str)))
		h.Write([]byte(str))
	}

	return hex.EncodeToString(h.Sum(nil))
}
