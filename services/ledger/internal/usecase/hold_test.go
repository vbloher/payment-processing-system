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

func Test_Hold(t *testing.T) {
	otel.SilentModeInit()

	paymentID := uuid.New()

	tests := []struct {
		name         string
		balance      int64
		amount       int64
		currency     string
		accountErr   error
		hold         func(account domain.Account) (domain.Hold, error)
		wantErr      error
		wantWrites   bool
		wantNoLookup bool
	}{
		{
			name: "new hold", balance: 10000, amount: 3000, currency: "EUR",
			hold:       func(domain.Account) (domain.Hold, error) { return domain.Hold{}, domain.ErrNotFound },
			wantWrites: true,
		},
		{
			name: "insufficient funds", balance: 1000, amount: 3000, currency: "EUR",
			hold:    func(domain.Account) (domain.Hold, error) { return domain.Hold{}, domain.ErrNotFound },
			wantErr: domain.ErrInsufficientFunds,
		},
		{
			name: "replay", balance: 10000, amount: 3000, currency: "EUR",
			hold: func(a domain.Account) (domain.Hold, error) {
				return holdOf(paymentID, a, 3000, domain.HoldActive), nil
			},
		},
		{
			name: "key reused", balance: 10000, amount: 3000, currency: "EUR",
			hold: func(a domain.Account) (domain.Hold, error) {
				return holdOf(paymentID, a, 2000, domain.HoldActive), nil
			},
			wantErr: domain.ErrIdempotencyConflict,
		},
		{
			name: "unknown account", balance: 10000, amount: 3000, currency: "EUR",
			accountErr: domain.ErrNotFound,
			wantErr:    domain.ErrNotFound,
		},
		{
			name: "other currency", balance: 10000, amount: 3000, currency: "USD",
			hold:    func(domain.Account) (domain.Hold, error) { return domain.Hold{}, domain.ErrNotFound },
			wantErr: domain.ErrCurrencyMismatch,
		},
		{
			name: "bad amount", balance: 10000, amount: 0, currency: "EUR",
			wantErr: domain.ErrValidation, wantNoLookup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := customer(tt.balance, 0)
			postgres := mocks.NewPostgres(t)

			if !tt.wantNoLookup {
				postgres.EXPECT().GetAccountForUpdate(Any, account.ID).Return(account, tt.accountErr)
			}

			if tt.hold != nil {
				h, err := tt.hold(account)
				postgres.EXPECT().GetHold(Any, paymentID).Return(h, err)
			}

			if tt.wantWrites {
				postgres.EXPECT().CreateHold(Any, mock.MatchedBy(func(h domain.Hold) bool {
					return h.PaymentID == paymentID && h.AccountID == account.ID &&
						h.Amount == 3000 && h.Status == domain.HoldActive
				})).Return(nil)
				postgres.EXPECT().UpdateAccountBalances(Any, accountWith(account.ID, tt.balance, 3000)).Return(nil)
			}

			u := usecase.New(newTransactor(t), postgres)

			err := u.Hold(t.Context(), dto.HoldInput{
				PaymentID: paymentID, AccountID: account.ID, Amount: tt.amount, Currency: tt.currency,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
