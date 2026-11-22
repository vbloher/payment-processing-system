package v1

import (
	http_server "github.com/vbloher/payment-processing-system/gen/mockbank/http/server"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/usecase"
)

var _ http_server.StrictServerInterface = (*Handlers)(nil)

type Handlers struct {
	usecase *usecase.UseCase
}

func New(uc *usecase.UseCase) *Handlers {
	return &Handlers{usecase: uc}
}
