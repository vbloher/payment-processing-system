//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/pkg/events"

	bank_client "github.com/vbloher/payment-processing-system/gen/mockbank/http/client"
	payment_client "github.com/vbloher/payment-processing-system/gen/payment/http/client"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	waitTimeout = 30 * time.Second
	waitTick    = 100 * time.Millisecond
)

// openAccount opens a ledger account and deposits into it when deposit is positive.
func (s *Suite) openAccount(currency string, deposit int64) uuid.UUID {
	ctx := context.Background()

	opened, err := s.ledger.OpenAccount(ctx, &ledger_v1.OpenAccountInput{Currency: currency})
	s.NoError(err)

	if deposit > 0 {
		_, err = s.ledger.Deposit(ctx, &ledger_v1.DepositInput{
			AccountId: opened.GetId(), Amount: deposit, Currency: currency, IdempotencyKey: uuid.NewString(),
		})
		s.NoError(err)
	}

	return uuid.MustParse(opened.GetId())
}

// createPayment posts a payment and returns its ID (uuid.Nil unless the answer is 202) and the HTTP status.
func (s *Suite) createPayment(
	key string, accountID uuid.UUID, amount int64, currency, beneficiary string,
) (id uuid.UUID, httpStatus int) {
	id, httpStatus, err := s.postPayment(key, accountID, amount, currency, beneficiary)
	s.NoError(err)

	return id, httpStatus
}

// postPayment is createPayment without assertions, so that goroutines other than the test's can call it.
func (s *Suite) postPayment(
	key string, accountID uuid.UUID, amount int64, currency, beneficiary string,
) (uuid.UUID, int, error) {
	res, err := s.payments.CreatePaymentWithResponse(context.Background(),
		&payment_client.CreatePaymentParams{IdempotencyKey: key},
		payment_client.CreatePaymentJSONRequestBody{
			AccountID: accountID, Amount: amount, Currency: currency, Beneficiary: beneficiary,
		})
	if err != nil {
		return uuid.Nil, 0, err
	}

	if res.JSON202 == nil {
		return uuid.Nil, res.StatusCode(), nil
	}

	return res.JSON202.ID, res.StatusCode(), nil
}

// waitStatus polls the payment until it has the wanted status and returns it.
func (s *Suite) waitStatus(id uuid.UUID, want string) payment_client.Payment {
	return s.waitPayment(id, func(c *assert.CollectT, p payment_client.Payment) {
		assert.Equal(c, want, string(p.Status))
	})
}

// waitFinal polls the payment until it is succeeded or failed and returns it.
func (s *Suite) waitFinal(id uuid.UUID) payment_client.Payment {
	return s.waitPayment(id, func(c *assert.CollectT, p payment_client.Payment) {
		assert.Contains(c, []payment_client.PaymentStatus{
			payment_client.PaymentStatusSucceeded, payment_client.PaymentStatusFailed,
		}, p.Status)
	})
}

func (s *Suite) waitPayment(id uuid.UUID, ok func(c *assert.CollectT, p payment_client.Payment)) payment_client.Payment {
	var got payment_client.Payment

	s.EventuallyWithT(func(c *assert.CollectT) {
		res, err := s.payments.GetPaymentWithResponse(context.Background(), id)
		if !assert.NoError(c, err) || !assert.Equal(c, http.StatusOK, res.StatusCode()) {
			return
		}

		got = *res.JSON200
		ok(c, got)
	}, waitTimeout, waitTick, "payment %s", id)

	return got
}

// balance returns the ledger balance of the account.
func (s *Suite) balance(accountID uuid.UUID) (balance, held, available int64) {
	out, err := s.ledger.GetBalance(context.Background(), &ledger_v1.GetBalanceInput{AccountId: accountID.String()})
	s.NoError(err)

	return out.GetBalance(), out.GetHeld(), out.GetAvailable()
}

// bankTransferStatus returns the bank's status of the transfer, "" when the bank answers 404.
func (s *Suite) bankTransferStatus(reference string) string {
	res, err := s.bank.GetTransferWithResponse(context.Background(), reference)
	s.NoError(err)

	switch res.StatusCode() {
	case http.StatusOK:
		return string(res.JSON200.Status)
	case http.StatusNotFound:
		return ""
	default:
		s.Failf("unexpected bank answer", "GET /transfers/%s: %d %s", reference, res.StatusCode(), res.Body)

		return ""
	}
}

// getPayment returns the payment as the API answers it now.
func (s *Suite) getPayment(id uuid.UUID) payment_client.Payment {
	res, err := s.payments.GetPaymentWithResponse(context.Background(), id)
	s.NoError(err)
	s.Equal(http.StatusOK, res.StatusCode(), string(res.Body))

	return *res.JSON200
}

// reconcile runs one reconciliation to completion. test/e2e cannot import the payment service's saga
// package, so the workflow and its task queue are named here.
func (s *Suite) reconcile() {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	run, err := s.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{TaskQueue: "reconciliation"}, "ReconciliationWorkflow")
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
}

// setBankAvailable turns the whole mock bank on or off.
func (s *Suite) setBankAvailable(available bool) {
	s.NoError(s.putBankAvailability(available))
}

// restoreBankOnCleanup turns the bank back on when the running test ends, whatever its outcome. The
// cleanup runs after the subtest's assertions are unbound, so it reports to the test that registered it.
func (s *Suite) restoreBankOnCleanup() {
	t := s.T()
	t.Cleanup(func() { assert.NoError(t, s.putBankAvailability(true)) })
}

func (s *Suite) putBankAvailability(available bool) error {
	res, err := s.bank.SetAvailabilityWithResponse(context.Background(),
		bank_client.SetAvailabilityJSONRequestBody{Available: available})
	if err != nil {
		return fmt.Errorf("PUT /admin/availability: %w", err)
	}

	if res.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("PUT /admin/availability: %d %s", res.StatusCode(), res.Body)
	}

	return nil
}

// setTransferAmount changes the amount of the bank's transfer, so that it drifts from the payment.
func (s *Suite) setTransferAmount(reference string, amount int64) {
	res, err := s.bank.SetTransferAmountWithResponse(context.Background(), reference,
		bank_client.SetTransferAmountJSONRequestBody{Amount: amount})
	s.NoError(err)
	s.Equal(http.StatusOK, res.StatusCode(), string(res.Body))
}

// discrepancies lists the discrepancies with the status through the payment API.
func (s *Suite) discrepancies(status payment_client.DiscrepancyStatus) []payment_client.Discrepancy {
	res, err := s.payments.GetDiscrepanciesWithResponse(context.Background(),
		&payment_client.GetDiscrepanciesParams{Status: &status})
	s.NoError(err)
	s.Equal(http.StatusOK, res.StatusCode(), string(res.Body))

	return res.JSON200.Discrepancies
}

// openDiscrepanciesOf returns the open discrepancies recorded for the reference.
func (s *Suite) openDiscrepanciesOf(reference string) []payment_client.Discrepancy {
	var found []payment_client.Discrepancy

	for _, d := range s.discrepancies(payment_client.DiscrepancyStatusOpen) {
		if d.Reference == reference {
			found = append(found, d)
		}
	}

	return found
}

// openDiscrepancies returns the sorted kinds of the open discrepancies recorded for the reference.
func (s *Suite) openDiscrepancies(reference string) []string {
	var kinds []string

	for _, d := range s.openDiscrepanciesOf(reference) {
		kinds = append(kinds, string(d.Kind))
	}

	slices.Sort(kinds)

	return kinds
}

// discrepancyRows counts the discrepancy rows, open or resolved, recorded for the reference.
func (s *Suite) discrepancyRows(reference string) int {
	var n int

	s.NoError(s.paymentDB.QueryRow(context.Background(),
		"SELECT count(*) FROM reconciliation_discrepancy WHERE reference = $1", reference).Scan(&n))

	return n
}

// requireLedgerIntact checks that every balance equals the sum of its entries
// and that debits equal credits within every currency.
func (s *Suite) requireLedgerIntact() {
	const balancesMatchEntries = `
SELECT count(*) FROM account a
WHERE a.balance <> COALESCE((SELECT sum(CASE WHEN e.side = a.normal_side THEN e.amount ELSE -e.amount END)
                             FROM ledger_entry e WHERE e.account_id = a.id), 0)`

	const currenciesBalance = `
SELECT count(*) FROM (SELECT currency, sum(CASE WHEN normal_side = 'debit' THEN balance ELSE -balance END) AS diff
                      FROM account GROUP BY currency) t WHERE diff <> 0`

	for _, query := range []string{balancesMatchEntries, currenciesBalance} {
		var broken int

		s.NoError(s.ledgerDB.QueryRow(context.Background(), query).Scan(&broken))
		s.Zero(broken, query)
	}
}

// countPayments returns the number of rows in the payment table.
func (s *Suite) countPayments() int {
	var n int

	s.NoError(s.paymentDB.QueryRow(context.Background(), "SELECT count(*) FROM payment").Scan(&n))

	return n
}

// webhookCall is a notification the webhook recorder received.
type webhookCall struct {
	IdempotencyKey string
	Envelope       events.Envelope
}

// webhookRecorder is the notification service's webhook: it records every call and answers 200.
type webhookRecorder struct {
	*httptest.Server

	mu    sync.Mutex
	calls []webhookCall
}

func newWebhookRecorder(t *testing.T) *webhookRecorder {
	w := &webhookRecorder{}

	w.Server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		e, err := events.Unmarshal(body)
		if err != nil {
			t.Errorf("webhook: %s %s: %v", r.Method, body, err)
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		w.mu.Lock()
		w.calls = append(w.calls, webhookCall{IdempotencyKey: r.Header.Get("Idempotency-Key"), Envelope: e})
		w.mu.Unlock()

		rw.WriteHeader(http.StatusOK)
	}))

	return w
}

func (w *webhookRecorder) filter(keep func(c webhookCall) bool) []webhookCall {
	w.mu.Lock()
	defer w.mu.Unlock()

	var calls []webhookCall

	for _, c := range w.calls {
		if keep(c) {
			calls = append(calls, c)
		}
	}

	return calls
}

// webhookCalls returns the webhook calls received so far for the payment.
func (s *Suite) webhookCalls(paymentID uuid.UUID) []webhookCall {
	return s.webhook.filter(func(c webhookCall) bool { return c.Envelope.PaymentID == paymentID })
}

// webhookCallsOfType returns the webhook calls received so far for the event type.
func (s *Suite) webhookCallsOfType(t events.Type) []webhookCall {
	return s.webhook.filter(func(c webhookCall) bool { return c.Envelope.Type == t })
}

// discrepancyEvents returns the reconciliation.discrepancy_found webhook calls received so far for the discrepancy.
func (s *Suite) discrepancyEvents(discrepancyID uuid.UUID) []webhookCall {
	return s.webhook.filter(func(c webhookCall) bool {
		if c.Envelope.Type != events.TypeDiscrepancyFound {
			return false
		}

		var data events.DiscrepancyData

		return json.Unmarshal(c.Envelope.Data, &data) == nil && data.DiscrepancyID == discrepancyID
	})
}

// waitWebhookCall waits for the first webhook call for the payment and returns it.
func (s *Suite) waitWebhookCall(paymentID uuid.UUID) webhookCall {
	var calls []webhookCall

	s.Eventually(func() bool {
		calls = s.webhookCalls(paymentID)

		return len(calls) > 0
	}, waitTimeout, waitTick, "no webhook call for payment %s", paymentID)

	return calls[0]
}

// endedSpan returns the ended span with the name and the messaging.message.id, or nil.
func (s *Suite) endedSpan(name, messageID string) sdktrace.ReadOnlySpan {
	for _, span := range s.spans.Ended() {
		if span.Name() != name {
			continue
		}

		for _, attr := range span.Attributes() {
			if attr.Key == "messaging.message.id" && attr.Value.AsString() == messageID {
				return span
			}
		}
	}

	return nil
}

// spanNames returns the names of the ended spans of the trace.
func (s *Suite) spanNames(traceID trace.TraceID) []string {
	var names []string

	for _, span := range s.spans.Ended() {
		if span.SpanContext().TraceID() == traceID {
			names = append(names, span.Name())
		}
	}

	return names
}
