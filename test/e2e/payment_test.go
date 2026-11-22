//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"

	payment_client "github.com/vbloher/payment-processing-system/gen/payment/http/client"
)

const amount = 3000

func (s *Suite) Test_Payment_Outcomes() {
	tests := []struct {
		name           string
		deposit        int64
		unknownAccount bool
		currency       string
		beneficiary    string
		wantStatus     string
		wantReason     string
		wantBalance    int64
		wantHeld       int64
		wantBank       string
	}{
		{name: "success", deposit: 10000, currency: "EUR", beneficiary: "DE-OK-1",
			wantStatus: "succeeded", wantBalance: 7000, wantBank: "succeeded"},
		{name: "insufficient funds", deposit: 1000, currency: "EUR", beneficiary: "DE-OK-2",
			wantStatus: "failed", wantReason: "insufficient_funds", wantBalance: 1000},
		{name: "bank decline", deposit: 10000, currency: "EUR", beneficiary: "DECLINE-1",
			wantStatus: "failed", wantReason: "bank_declined", wantBalance: 10000, wantBank: "declined"},
		{name: "transient bank errors", deposit: 10000, currency: "EUR", beneficiary: "FLAKY-1",
			wantStatus: "succeeded", wantBalance: 7000, wantBank: "succeeded"},
		{name: "timeout after processing", deposit: 10000, currency: "EUR", beneficiary: "TIMEOUT-1",
			wantStatus: "succeeded", wantBalance: 7000, wantBank: "succeeded"},
		{name: "unknown account", unknownAccount: true, currency: "EUR", beneficiary: "DE-OK-3",
			wantStatus: "failed", wantReason: "account_not_found"},
		{name: "other currency", deposit: 10000, currency: "USD", beneficiary: "DE-OK-4",
			wantStatus: "failed", wantReason: "ledger_rejected", wantBalance: 10000},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			account := uuid.New()
			if !tt.unknownAccount {
				account = s.openAccount("EUR", tt.deposit)
			}

			id, code := s.createPayment(uuid.NewString(), account, amount, tt.currency, tt.beneficiary)
			s.Equal(http.StatusAccepted, code)

			p := s.waitStatus(id, tt.wantStatus)
			s.Equal(tt.wantReason, p.FailureReason)

			if tt.wantStatus == "succeeded" {
				s.NotEmpty(p.BankReference)
			}

			if !tt.unknownAccount {
				balance, held, _ := s.balance(account)
				s.Equal(tt.wantBalance, balance, "balance")
				s.Equal(tt.wantHeld, held, "held")
			}

			s.Equal(tt.wantBank, s.bankTransferStatus(id.String()), "bank transfer")
			s.requireLedgerIntact()
		})
	}
}

func (s *Suite) Test_Payment_Idempotency() {
	s.Run("same key", func() {
		account := s.openAccount("EUR", 10000)
		key := uuid.NewString()

		first, code := s.createPayment(key, account, amount, "EUR", "DE-OK-1")
		s.Equal(http.StatusAccepted, code)

		second, code := s.createPayment(key, account, amount, "EUR", "DE-OK-1")
		s.Equal(http.StatusAccepted, code)
		s.Equal(first, second)

		_, code = s.createPayment(key, account, amount+1, "EUR", "DE-OK-1")
		s.Equal(http.StatusConflict, code)

		s.waitStatus(first, "succeeded")

		// Wait until the workflow run has closed: a replay then meets a finished run, which the
		// start must treat as "already started", not as an error.
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		var status string

		s.NoError(s.temporal.GetWorkflow(ctx, "payment-"+first.String(), "").Get(ctx, &status))
		s.Equal("succeeded", status)

		third, code := s.createPayment(key, account, amount, "EUR", "DE-OK-1")
		s.Equal(http.StatusAccepted, code)
		s.Equal(first, third)

		balance, held, _ := s.balance(account)
		s.EqualValues(7000, balance)
		s.Zero(held)
	})

	s.Run("concurrent requests with the same key", func() {
		account := s.openAccount("EUR", 10000)
		key := uuid.NewString()

		results := s.postConcurrently(10, func(int) (uuid.UUID, int, error) {
			return s.postPayment(key, account, amount, "EUR", "DE-OK-1")
		})

		id := results[0].id
		for _, r := range results {
			s.NoError(r.err)
			s.Equal(http.StatusAccepted, r.code)
			s.Equal(id, r.id)
		}

		var rows int

		s.NoError(s.paymentDB.QueryRow(context.Background(),
			"SELECT count(*) FROM payment WHERE idempotency_key = $1", key).Scan(&rows))
		s.Equal(1, rows)

		s.waitStatus(id, "succeeded")

		balance, held, _ := s.balance(account)
		s.EqualValues(7000, balance)
		s.Zero(held)
		s.requireLedgerIntact()
	})
}

func (s *Suite) Test_Payment_Validation() {
	tests := []struct {
		name   string
		key    string // "" sends no Idempotency-Key header
		mutate func(body map[string]any)
	}{
		{name: "no Idempotency-Key header", key: ""},
		{name: "Idempotency-Key of 256 characters", key: strings.Repeat("k", 256)},
		{name: "amount 0", key: uuid.NewString(), mutate: func(b map[string]any) { b["amount"] = 0 }},
		{name: "amount -5", key: uuid.NewString(), mutate: func(b map[string]any) { b["amount"] = -5 }},
		{name: "amount 1000000000001", key: uuid.NewString(),
			mutate: func(b map[string]any) { b["amount"] = int64(1_000_000_000_001) }},
		{name: "currency eur", key: uuid.NewString(), mutate: func(b map[string]any) { b["currency"] = "eur" }},
		{name: "account_id nope", key: uuid.NewString(), mutate: func(b map[string]any) { b["account_id"] = "nope" }},
		{name: "empty beneficiary", key: uuid.NewString(), mutate: func(b map[string]any) { b["beneficiary"] = "" }},
	}

	// A funded account, so that a request that slipped through validation would create a payment.
	account := s.openAccount("EUR", 10000)
	before := s.countPayments()

	for _, tt := range tests {
		s.Run(tt.name, func() {
			body := map[string]any{
				"account_id": account.String(), "amount": amount, "currency": "EUR", "beneficiary": "DE-OK-1",
			}
			if tt.mutate != nil {
				tt.mutate(body)
			}

			raw, err := json.Marshal(body)
			s.NoError(err)

			res, err := s.payments.CreatePaymentWithBodyWithResponse(context.Background(),
				&payment_client.CreatePaymentParams{IdempotencyKey: tt.key}, "application/json", bytes.NewReader(raw),
				func(_ context.Context, req *http.Request) error {
					if tt.key == "" {
						req.Header.Del("Idempotency-Key")
					}

					return nil
				})
			s.NoError(err)
			s.Equal(http.StatusBadRequest, res.StatusCode(), string(res.Body))
			s.Equal(before, s.countPayments())
		})
	}
}

func (s *Suite) Test_Payment_Get() {
	s.Run("unknown id", func() {
		res, err := s.payments.GetPaymentWithResponse(context.Background(), uuid.New())
		s.NoError(err)
		s.Equal(http.StatusNotFound, res.StatusCode())
		s.NotNil(res.JSON404)
	})

	s.Run("malformed id", func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			s.paymentURL+"/payments/nope", http.NoBody)
		s.NoError(err)

		resp, err := http.DefaultClient.Do(req)
		s.NoError(err)

		defer func() { _ = resp.Body.Close() }()

		s.Equal(http.StatusBadRequest, resp.StatusCode)
	})
}

// Test_Payment_ConcurrentOverdraft is spec scenario 10: parallel payments over the balance.
func (s *Suite) Test_Payment_ConcurrentOverdraft() {
	account := s.openAccount("EUR", 10000)

	results := s.postConcurrently(10, func(int) (uuid.UUID, int, error) {
		return s.postPayment(uuid.NewString(), account, amount, "EUR", "DE-OK-1")
	})

	var succeeded, insufficient int

	for _, r := range results {
		s.NoError(r.err)
		s.Equal(http.StatusAccepted, r.code)

		p := s.waitFinal(r.id)

		switch {
		case p.Status == payment_client.PaymentStatusSucceeded:
			succeeded++
		case p.Status == payment_client.PaymentStatusFailed && p.FailureReason == "insufficient_funds":
			insufficient++
		default:
			s.Failf("unexpected outcome", "payment %s: %s %q", p.ID, p.Status, p.FailureReason)
		}
	}

	s.Equal(3, succeeded)
	s.Equal(7, insufficient)

	balance, held, _ := s.balance(account)
	s.EqualValues(1000, balance)
	s.Zero(held)
	s.requireLedgerIntact()
}

type postResult struct {
	id   uuid.UUID
	code int
	err  error
}

// postConcurrently runs post n times in parallel and returns the results in call order.
func (s *Suite) postConcurrently(n int, post func(i int) (uuid.UUID, int, error)) []postResult {
	results := make([]postResult, n)

	var wg sync.WaitGroup

	for i := range results {
		wg.Go(func() {
			id, code, err := post(i)
			results[i] = postResult{id: id, code: code, err: err}
		})
	}

	wg.Wait()

	return results
}
