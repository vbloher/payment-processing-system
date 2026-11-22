//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
)

func (s *Suite) Test_SaveInbox_Deduplicates() {
	ctx := context.Background()
	id := uuid.New()

	inserted, err := s.pg.SaveInbox(ctx, id, "payment.succeeded")
	s.NoError(err)
	s.True(inserted)

	inserted, err = s.pg.SaveInbox(ctx, id, "payment.succeeded")
	s.NoError(err)
	s.False(inserted)
}

func (s *Suite) Test_SaveInbox_ConcurrentSameEvent() {
	ctx := context.Background()
	id := uuid.New()

	const workers = 20

	var (
		wg    sync.WaitGroup
		wins  atomic.Int32
		fails atomic.Int32
	)

	for range workers {
		wg.Go(func() {
			inserted, err := s.pg.SaveInbox(ctx, id, "payment.succeeded")
			if err != nil {
				fails.Add(1)

				return
			}

			if inserted {
				wins.Add(1)
			}
		})
	}

	wg.Wait()

	s.Zero(fails.Load())
	s.EqualValues(1, wins.Load())
}

func (s *Suite) Test_Notification_Lifecycle() {
	ctx := context.Background()

	e, err := events.New(events.TypePaymentFailed, uuid.New(), events.PaymentData{Amount: 500, Currency: "EUR", Status: "failed"})
	s.NoError(err)

	n, err := domain.NewNotification(e)
	s.NoError(err)

	inserted, err := s.pg.SaveInbox(ctx, e.ID, string(e.Type))
	s.NoError(err)
	s.True(inserted)

	s.NoError(s.pg.CreateNotification(ctx, n))

	got, err := s.pg.GetNotificationByEventID(ctx, e.ID)
	s.NoError(err)
	s.Equal(n.ID, got.ID)
	s.Equal(e.ID, got.EventID)
	s.Equal(string(e.Type), got.EventType)
	s.Equal(e.PaymentID, got.PaymentID)
	s.Equal(domain.StatusPending, got.Status)
	s.JSONEq(string(n.Payload), string(got.Payload))

	s.NoError(s.pg.MarkNotificationSent(ctx, n.ID))

	got, err = s.pg.GetNotificationByEventID(ctx, e.ID)
	s.NoError(err)
	s.Equal(domain.StatusSent, got.Status)

	var sentAtSet bool

	s.NoError(s.pool.QueryRow(ctx, "SELECT sent_at IS NOT NULL FROM notification WHERE id = $1", n.ID).Scan(&sentAtSet))
	s.True(sentAtSet)

	_, err = s.pg.GetNotificationByEventID(ctx, uuid.New())
	s.ErrorIs(err, domain.ErrNotFound)

	s.ErrorIs(s.pg.MarkNotificationSent(ctx, uuid.New()), domain.ErrNotFound)
}
