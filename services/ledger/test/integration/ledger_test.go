//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (s *Suite) openFunded(amount int64, key string) uuid.UUID {
	ctx := context.Background()

	out, err := s.uc.OpenAccount(ctx, dto.OpenAccountInput{Currency: "EUR"})
	s.NoError(err)

	s.NoError(s.uc.Deposit(ctx, dto.DepositInput{
		AccountID: out.ID, Amount: amount, Currency: "EUR", IdempotencyKey: key,
	}))

	return out.ID
}

func (s *Suite) requireBalance(id uuid.UUID, balance, held, available int64) {
	got, err := s.uc.GetBalance(context.Background(), dto.GetBalanceInput{AccountID: id})
	s.NoError(err)
	s.Equal(dto.GetBalanceOutput{Balance: balance, Held: held, Available: available, Currency: "EUR"}, got)
}

func (s *Suite) Test_HoldCaptureRelease_Balances() {
	ctx := context.Background()
	id := s.openFunded(10000, "dep-1")

	pay1, pay2 := uuid.New(), uuid.New()

	s.NoError(s.uc.Hold(ctx, dto.HoldInput{PaymentID: pay1, AccountID: id, Amount: 3000, Currency: "EUR"}))
	s.requireBalance(id, 10000, 3000, 7000)

	s.NoError(s.uc.Capture(ctx, dto.CaptureInput{PaymentID: pay1}))
	s.requireBalance(id, 7000, 0, 7000)

	s.NoError(s.uc.Hold(ctx, dto.HoldInput{PaymentID: pay2, AccountID: id, Amount: 2000, Currency: "EUR"}))
	s.NoError(s.uc.Release(ctx, dto.ReleaseInput{PaymentID: pay2}))
	s.requireBalance(id, 7000, 0, 7000)

	s.requireLedgerIntact()
}

func (s *Suite) Test_Deposit_Idempotent() {
	ctx := context.Background()

	out, err := s.uc.OpenAccount(ctx, dto.OpenAccountInput{Currency: "EUR"})
	s.NoError(err)

	in := dto.DepositInput{AccountID: out.ID, Amount: 10000, Currency: "EUR", IdempotencyKey: "dep-same"}
	s.NoError(s.uc.Deposit(ctx, in))
	s.NoError(s.uc.Deposit(ctx, in))

	in.Amount = 20000
	s.ErrorIs(s.uc.Deposit(ctx, in), domain.ErrIdempotencyConflict)

	s.requireBalance(out.ID, 10000, 0, 10000)
	s.requireLedgerIntact()
}

func (s *Suite) Test_Hold_Concurrent_NeverOverdraws() {
	ctx := context.Background()
	id := s.openFunded(10000, "dep-concurrent-hold")

	const workers = 20

	errs := make([]error, workers)

	var wg sync.WaitGroup

	for i := range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs[i] = s.uc.Hold(ctx, dto.HoldInput{PaymentID: uuid.New(), AccountID: id, Amount: 3000, Currency: "EUR"})
		}()
	}

	wg.Wait()

	var succeeded, insufficient int

	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, domain.ErrInsufficientFunds):
			insufficient++
		default:
			s.Failf("unexpected error", "%v", err)
		}
	}

	s.Equal(3, succeeded)
	s.Equal(workers-3, insufficient)
	s.requireBalance(id, 10000, 9000, 1000)
	s.requireLedgerIntact()
}

func (s *Suite) Test_Capture_Concurrent_SamePayment() {
	ctx := context.Background()
	id := s.openFunded(10000, "dep-concurrent-capture")
	paymentID := uuid.New()

	s.NoError(s.uc.Hold(ctx, dto.HoldInput{PaymentID: paymentID, AccountID: id, Amount: 3000, Currency: "EUR"}))

	const workers = 5

	errs := make([]error, workers)

	var wg sync.WaitGroup

	for i := range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs[i] = s.uc.Capture(ctx, dto.CaptureInput{PaymentID: paymentID})
		}()
	}

	wg.Wait()

	for _, err := range errs {
		s.NoError(err)
	}

	var count int

	s.NoError(s.pool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_transaction WHERE reference = $1`, paymentID.String()).Scan(&count))
	s.Equal(1, count)

	s.requireBalance(id, 7000, 0, 7000)
	s.requireLedgerIntact()
}

func (s *Suite) Test_LedgerEntries_AreImmutable() {
	ctx := context.Background()
	s.openFunded(1000, "dep-immutable")

	_, err := s.pool.Exec(ctx, `UPDATE ledger_entry SET amount = 1`)
	s.ErrorContains(err, "immutable")

	_, err = s.pool.Exec(ctx, `DELETE FROM ledger_entry`)
	s.ErrorContains(err, "immutable")
}
