package v1

import (
	"context"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (h Handlers) GetBalance(ctx context.Context, i *pb.GetBalanceInput) (*pb.GetBalanceOutput, error) {
	accountID, err := parseUUID(ctx, i.GetAccountId())
	if err != nil {
		return nil, err
	}

	output, err := h.usecase.GetBalance(ctx, dto.GetBalanceInput{AccountID: accountID})
	if err != nil {
		return nil, grpcError(ctx, err)
	}

	return &pb.GetBalanceOutput{
		Balance:   output.Balance,
		Held:      output.Held,
		Available: output.Available,
		Currency:  output.Currency,
	}, nil
}
