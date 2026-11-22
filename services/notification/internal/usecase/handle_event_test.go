package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/notification/internal/domain"
	"github.com/vbloher/payment-processing-system/services/notification/internal/usecase"
	"github.com/vbloher/payment-processing-system/services/notification/internal/usecase/mocks"
)

const Any = mock.Anything

func newTransactor(t *testing.T) *mocks.Transactor {
	t.Helper()

	tx := mocks.NewTransactor(t)
	tx.EXPECT().Wrap(Any, Any).RunAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	}).Maybe()

	return tx
}

func Test_HandleEvent(t *testing.T) {
	otel.SilentModeInit()

	errBoom := errors.New("boom")

	tests := []struct {
		name       string
		inserted   bool
		inboxErr   error
		stored     domain.Status
		sendErr    error
		wantErr    error
		wantCreate int
		wantSend   int
		wantMark   int
	}{
		{name: "first delivery", inserted: true, stored: domain.StatusPending, wantCreate: 1, wantSend: 1, wantMark: 1},
		{name: "duplicate, already sent", stored: domain.StatusSent},
		{name: "duplicate, not yet sent", stored: domain.StatusPending, wantSend: 1, wantMark: 1},
		{name: "sender fails", inserted: true, stored: domain.StatusPending, sendErr: errBoom, wantErr: errBoom, wantCreate: 1, wantSend: 1},
		{name: "inbox write fails", inboxErr: errBoom, wantErr: errBoom},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := events.New(events.TypePaymentSucceeded, uuid.New(), events.PaymentData{Amount: 100, Currency: "EUR"})
			require.NoError(t, err)

			stored := domain.Notification{ID: uuid.New(), EventID: e.ID, Status: tt.stored}

			postgres := mocks.NewPostgres(t)
			postgres.EXPECT().SaveInbox(Any, e.ID, string(e.Type)).Return(tt.inserted, tt.inboxErr).Once()

			if tt.wantCreate > 0 {
				postgres.EXPECT().CreateNotification(Any, mock.MatchedBy(func(n domain.Notification) bool {
					return n.EventID == e.ID && n.Status == domain.StatusPending
				})).Return(nil).Times(tt.wantCreate)
			}

			if tt.inboxErr == nil {
				postgres.EXPECT().GetNotificationByEventID(Any, e.ID).Return(stored, nil).Once()
			}

			sender := mocks.NewSender(t)
			if tt.wantSend > 0 {
				sender.EXPECT().Send(Any, mock.MatchedBy(func(n domain.Notification) bool {
					return n.EventID == e.ID
				})).Return(tt.sendErr).Times(tt.wantSend)
			}

			if tt.wantMark > 0 {
				postgres.EXPECT().MarkNotificationSent(Any, stored.ID).Return(nil).Times(tt.wantMark)
			}

			uc := usecase.New(newTransactor(t), postgres, sender)

			err = uc.HandleEvent(context.Background(), e)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
