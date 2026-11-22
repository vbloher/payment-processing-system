package usecase_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase/mocks"
)

func Test_Deposit(t *testing.T) {
	otel.SilentModeInit()

	tests := []struct {
		name    string
		amount  int64
		created bool
		wantErr error
		noCalls bool
	}{
		{name: "new", amount: 5000, created: true},
		{name: "duplicate", amount: 5000, created: false},
		{name: "bad amount", amount: 0, wantErr: domain.ErrValidation, noCalls: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := customer(10000, 0)
			bank := bankCash(10000)

			postgres := mocks.NewPostgres(t)

			if !tt.noCalls {
				postgres.EXPECT().GetAccountForUpdate(Any, account.ID).Return(account, nil)
				postgres.EXPECT().GetBankCashForUpdate(Any, "EUR").Return(bank, nil)
				postgres.EXPECT().CreateTransaction(Any, mock.MatchedBy(func(tx domain.Transaction) bool {
					return tx.Kind == domain.KindDeposit && tx.Reference == "dep-1" &&
						len(tx.Entries) == 2 &&
						tx.Entries[0] == domain.Entry{AccountID: bank.ID, Side: domain.Debit, Amount: 5000} &&
						tx.Entries[1] == domain.Entry{AccountID: account.ID, Side: domain.Credit, Amount: 5000}
				})).Return(tt.created, nil)
			}

			if tt.created {
				postgres.EXPECT().UpdateAccountBalances(Any, accountWith(account.ID, 15000, 0)).Return(nil)
				postgres.EXPECT().UpdateAccountBalances(Any, accountWith(bank.ID, 15000, 0)).Return(nil)
			}

			u := usecase.New(newTransactor(t), postgres)

			err := u.Deposit(t.Context(), dto.DepositInput{
				AccountID: account.ID, Amount: tt.amount, Currency: "EUR", IdempotencyKey: "dep-1",
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
