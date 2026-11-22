package v1

import (
	"context"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (h Handlers) OpenAccount(ctx context.Context, i *pb.OpenAccountInput) (*pb.OpenAccountOutput, error) {
	output, err := h.usecase.OpenAccount(ctx, dto.OpenAccountInput{Currency: i.GetCurrency()})
	if err != nil {
		return nil, grpcError(ctx, err)
	}

	return &pb.OpenAccountOutput{Id: output.ID.String()}, nil
}
