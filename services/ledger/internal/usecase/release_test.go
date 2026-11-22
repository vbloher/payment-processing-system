package usecase_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase/mocks"
)

func Test_Release(t *testing.T) {
	otel.SilentModeInit()

	tests := []struct {
		name    string
		status  domain.HoldStatus
		missing bool
		wantErr error
		writes  bool
	}{
		{name: "active", status: domain.HoldActive, writes: true},
		{name: "replay", status: domain.HoldReleased},
		{name: "after capture", status: domain.HoldCaptured, wantErr: domain.ErrHoldConflict},
		{name: "missing", missing: true, wantErr: domain.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paymentID := uuid.New()
			account := customer(10000, 3000)
			hold := holdOf(paymentID, account, 3000, tt.status)

			postgres := mocks.NewPostgres(t)

			if tt.missing {
				postgres.EXPECT().GetHold(Any, paymentID).Return(domain.Hold{}, domain.ErrNotFound)
			} else {
				postgres.EXPECT().GetHold(Any, paymentID).Return(hold, nil)
				postgres.EXPECT().GetAccountForUpdate(Any, account.ID).Return(account, nil)
				postgres.EXPECT().GetHoldForUpdate(Any, paymentID).Return(hold, nil)
			}

			if tt.writes {
				postgres.EXPECT().UpdateHoldStatus(Any, mock.MatchedBy(func(h domain.Hold) bool {
					return h.PaymentID == paymentID && h.Status == domain.HoldReleased
				})).Return(nil)
				postgres.EXPECT().UpdateAccountBalances(Any, accountWith(account.ID, 10000, 0)).Return(nil)
			}

			u := usecase.New(newTransactor(t), postgres)

			err := u.Release(t.Context(), dto.ReleaseInput{PaymentID: paymentID})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
