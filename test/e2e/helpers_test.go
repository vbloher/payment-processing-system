//go:build integration

package e2e

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"

	payment_client "github.com/vbloher/payment-processing-system/gen/payment/http/client"
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
