package dto

type CreateTransferInput struct {
	Reference             string
	Amount                int64
	Currency, Beneficiary string
}
