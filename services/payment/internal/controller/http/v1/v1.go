package v1

import (
	http_server "github.com/vbloher/payment-processing-system/gen/payment/http/server"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/usecase"
)

var _ http_server.StrictServerInterface = (*Handlers)(nil)

type Handlers struct {
	usecase *usecase.UseCase
}

func New(uc *usecase.UseCase) *Handlers {
	return &Handlers{usecase: uc}
}

func toPayment(p domain.Payment) http_server.Payment {
	return http_server.Payment{
		ID:            p.ID,
		AccountID:     p.AccountID,
		Amount:        p.Amount,
		Currency:      p.Currency,
		Beneficiary:   p.Beneficiary,
		Description:   p.Description,
		Status:        http_server.PaymentStatus(p.Status),
		FailureReason: string(p.FailureReason),
		BankReference: p.BankReference,
		CreatedAt:     p.CreatedAt.UTC(),
		UpdatedAt:     p.UpdatedAt.UTC(),
	}
}
