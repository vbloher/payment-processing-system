package dto

import (
	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

type CreatePaymentInput struct {
	IdempotencyKey                     string
	AccountID                          uuid.UUID
	Amount                             int64
	Currency, Beneficiary, Description string
}

type GetPaymentInput struct{ ID uuid.UUID }

type PaymentOutput struct{ Payment domain.Payment }
