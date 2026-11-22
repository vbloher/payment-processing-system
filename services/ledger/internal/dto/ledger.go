package dto

import (
	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

type OpenAccountInput struct{ Currency string }

type OpenAccountOutput struct{ ID uuid.UUID }

type DepositInput struct {
	AccountID                uuid.UUID
	Amount                   int64
	Currency, IdempotencyKey string
}

type GetBalanceInput struct{ AccountID uuid.UUID }

type GetBalanceOutput struct {
	Balance, Held, Available int64
	Currency                 string
}

type HoldInput struct {
	PaymentID, AccountID uuid.UUID
	Amount               int64
	Currency             string
}

type CaptureInput struct{ PaymentID uuid.UUID }

type ReleaseInput struct{ PaymentID uuid.UUID }

type GetHoldsInput struct{ PaymentIDs []uuid.UUID }

type GetHoldsOutput struct{ Holds []domain.Hold }
