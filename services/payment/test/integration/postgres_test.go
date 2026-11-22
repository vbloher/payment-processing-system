//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

var errRollback = errors.New("rollback")

func (s *Suite) newPayment(key string) domain.Payment {
	p, err := domain.NewPayment(key, uuid.New(), 1000, "EUR", "ACME", "invoice")
	s.NoError(err)

	return p
}

func (s *Suite) count(query string, args ...any) int {
	var n int

	s.NoError(s.pool.QueryRow(context.Background(), query, args...).Scan(&n))

	return n
}

func (s *Suite) Test_CreatePayment_IdempotencyKey() {
	ctx := context.Background()
	key := "key-" + uuid.NewString()
	first, second := s.newPayment(key), s.newPayment(key)

	created, err := s.pg.CreatePayment(ctx, first)
	s.NoError(err)
	s.True(created)

	created, err = s.pg.CreatePayment(ctx, second)
	s.NoError(err)
	s.False(created)

	got, err := s.pg.GetPaymentByIdempotencyKey(ctx, first.IdempotencyKey)
	s.NoError(err)
	s.Equal(first.ID, got.ID)
	s.Equal(first.RequestHash, got.RequestHash)
	s.Equal(domain.StatusCreated, got.Status)
}

func (s *Suite) Test_CreatePayment_ConcurrentSameKey() {
	ctx := context.Background()
	key := "concurrent-" + uuid.NewString()

	var (
		wg      sync.WaitGroup
		winners atomic.Int32
		failed  atomic.Int32
	)

	start := make(chan struct{})

	for range 20 {
		p := s.newPayment(key)

		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			created, err := s.pg.CreatePayment(ctx, p)
			if err != nil {
				failed.Add(1)

				return
			}

			if created {
				winners.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	s.Zero(failed.Load())
	s.Equal(int32(1), winners.Load())
	s.Equal(1, s.count("SELECT count(*) FROM payment WHERE idempotency_key = $1", key))
}

func (s *Suite) Test_UpdatePayment() {
	ctx := context.Background()
	p := s.newPayment("upd-" + uuid.NewString())

	created, err := s.pg.CreatePayment(ctx, p)
	s.NoError(err)
	s.True(created)

	// CreatePayment stores the app clock's timestamps, UpdatePayment the database clock's.
	// The two clocks differ by up to a millisecond, so compare two database-clock values:
	// a first update moves updated_at onto the database clock, the second must move it forward.
	p.Status = domain.StatusProcessing
	s.NoError(s.pg.UpdatePayment(ctx, p))

	before, err := s.pg.GetPayment(ctx, p.ID)
	s.NoError(err)

	p.Status = domain.StatusFailed
	p.FailureReason = domain.FailureReason("bank_declined")
	p.BankReference = "b-1"
	s.NoError(s.pg.UpdatePayment(ctx, p))

	got, err := s.pg.GetPayment(ctx, p.ID)
	s.NoError(err)
	s.Equal(domain.StatusFailed, got.Status)
	s.Equal(domain.FailureReason("bank_declined"), got.FailureReason)
	s.Equal("b-1", got.BankReference)
	s.True(got.UpdatedAt.After(before.UpdatedAt))
}

func (s *Suite) Test_GetPayment_NotFound() {
	ctx := context.Background()

	_, err := s.pg.GetPayment(ctx, uuid.New())
	s.ErrorIs(err, domain.ErrNotFound)

	_, err = s.pg.GetPaymentForUpdate(ctx, uuid.New())
	s.ErrorIs(err, domain.ErrNotFound)

	_, err = s.pg.GetPaymentByIdempotencyKey(ctx, "missing-"+uuid.NewString())
	s.ErrorIs(err, domain.ErrNotFound)

	s.ErrorIs(s.pg.UpdatePayment(ctx, s.newPayment("x")), domain.ErrNotFound)
}

func (s *Suite) Test_ListPayments() {
	ctx := context.Background()

	// Reviews from other tests would pollute the status listing; start from a clean table.
	_, err := s.pool.Exec(ctx, "DELETE FROM payment")
	s.NoError(err)

	payments := []domain.Payment{s.newPayment("l-1"), s.newPayment("l-2"), s.newPayment("l-3")}
	for _, p := range payments {
		created, createErr := s.pg.CreatePayment(ctx, p)
		s.NoError(createErr)
		s.True(created)
	}

	payments[1].Status = domain.StatusReview
	s.NoError(s.pg.UpdatePayment(ctx, payments[1]))

	review, err := s.pg.ListPaymentsByStatus(ctx, domain.StatusReview)
	s.NoError(err)
	s.Len(review, 1)
	s.Equal(payments[1].ID, review[0].ID)

	now := time.Now()

	between, err := s.pg.ListPaymentsUpdatedBetween(ctx, now.Add(-time.Minute), now.Add(time.Minute))
	s.NoError(err)
	s.Len(between, 3)

	past, err := s.pg.ListPaymentsUpdatedBetween(ctx, now.Add(-2*time.Hour), now.Add(-time.Hour))
	s.NoError(err)
	s.Empty(past)

	some, err := s.pg.GetPayments(ctx, []uuid.UUID{payments[0].ID, payments[2].ID})
	s.NoError(err)
	s.Len(some, 2)
}

func (s *Suite) Test_Outbox_SaveAndRead() {
	ctx := context.Background()

	_, err := s.pool.Exec(ctx, "DELETE FROM outbox")
	s.NoError(err)

	events := []domain.Event{
		{ID: uuid.New(), Subject: "payment.created", Payload: []byte(`{"a":1}`)},
		{ID: uuid.New(), Subject: "payment.succeeded", Payload: []byte(`{"b":2}`), Headers: map[string]string{"k": "v"}},
	}

	s.NoError(s.pg.SaveOutbox(ctx, events...))
	s.Equal(2, s.count("SELECT count(*) FROM outbox"))

	var got []domain.Event

	s.NoError(s.tx.Wrap(ctx, func(ctx context.Context) error {
		var readErr error

		got, readErr = s.pg.ReadOutbox(ctx, 10)

		return readErr
	}))

	s.Len(got, 2)

	for i, e := range events {
		s.Equal(e.ID, got[i].ID)
		s.Equal(e.Subject, got[i].Subject)
		s.Equal(e.Payload, got[i].Payload)
	}

	s.Equal("v", got[1].Headers["k"])
	s.Zero(s.count("SELECT count(*) FROM outbox"))

	dup := domain.Event{ID: uuid.New(), Subject: "payment.created", Payload: []byte("x")}
	s.NoError(s.pg.SaveOutbox(ctx, dup))
	s.Error(s.pg.SaveOutbox(ctx, dup))
}

func (s *Suite) Test_Outbox_RollbackKeepsEvents() {
	ctx := context.Background()

	_, err := s.pool.Exec(ctx, "DELETE FROM outbox")
	s.NoError(err)

	s.NoError(s.pg.SaveOutbox(ctx,
		domain.Event{ID: uuid.New(), Subject: "payment.created", Payload: []byte("1")},
		domain.Event{ID: uuid.New(), Subject: "payment.created", Payload: []byte("2")},
	))

	err = s.tx.Wrap(ctx, func(ctx context.Context) error {
		got, readErr := s.pg.ReadOutbox(ctx, 10)
		s.NoError(readErr)
		s.Len(got, 2)

		return errRollback
	})
	s.ErrorIs(err, errRollback)
	s.Equal(2, s.count("SELECT count(*) FROM outbox"))
}
