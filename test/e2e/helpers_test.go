//go:build integration

package e2e

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/pkg/events"

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
