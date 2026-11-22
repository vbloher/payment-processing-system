package v1

import (
	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/usecase"
)

type Handlers struct {
	pb.UnimplementedLedgerV1Server

	usecase *usecase.UseCase
}

func New(uc *usecase.UseCase) *Handlers {
	return &Handlers{
		usecase: uc,
	}
}
