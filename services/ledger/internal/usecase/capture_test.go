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

func Test_Capture(t *testing.T) {
	otel.SilentModeInit()

	tests := []struct {
		name    string
		status  domain.HoldStatus
		missing bool
		wantErr error
		writes  bool
		posted  bool // the payment's posting already exists: no balance or hold may change
	}{
		{name: "active", status: domain.HoldActive, writes: true},
		{name: "active, posting already exists", status: domain.HoldActive, writes: true, posted: true,
			wantErr: domain.ErrHoldConflict},
		{name: "replay", status: domain.HoldCaptured},
		{name: "after release", status: domain.HoldReleased, wantErr: domain.ErrHoldConflict},
		{name: "missing", missing: true, wantErr: domain.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paymentID := uuid.New()
			account := customer(10000, 3000)
			bank := bankCash(10000)
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
				postgres.EXPECT().GetBankCashForUpdate(Any, "EUR").Return(bank, nil)
				postgres.EXPECT().CreateTransaction(Any, mock.MatchedBy(func(tx domain.Transaction) bool {
					return tx.Kind == domain.KindPayment && tx.Reference == paymentID.String() &&
						len(tx.Entries) == 2 &&
						tx.Entries[0] == domain.Entry{AccountID: account.ID, Side: domain.Debit, Amount: 3000} &&
						tx.Entries[1] == domain.Entry{AccountID: bank.ID, Side: domain.Credit, Amount: 3000}
				})).Return(!tt.posted, nil)
			}

			if tt.writes && !tt.posted {
				postgres.EXPECT().UpdateHoldStatus(Any, mock.MatchedBy(func(h domain.Hold) bool {
					return h.PaymentID == paymentID && h.Status == domain.HoldCaptured
				})).Return(nil)
				postgres.EXPECT().UpdateAccountBalances(Any, accountWith(account.ID, 7000, 0)).Return(nil)
				postgres.EXPECT().UpdateAccountBalances(Any, accountWith(bank.ID, 7000, 0)).Return(nil)
			}

			u := usecase.New(newTransactor(t), postgres)

			err := u.Capture(t.Context(), dto.CaptureInput{PaymentID: paymentID})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
