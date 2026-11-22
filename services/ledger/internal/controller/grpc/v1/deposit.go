package v1

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (h Handlers) Deposit(ctx context.Context, i *pb.DepositInput) (*emptypb.Empty, error) {
	accountID, err := parseUUID(ctx, i.GetAccountId())
	if err != nil {
		return nil, err
	}

	err = h.usecase.Deposit(ctx, dto.DepositInput{
		AccountID:      accountID,
		Amount:         i.GetAmount(),
		Currency:       i.GetCurrency(),
		IdempotencyKey: i.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, grpcError(ctx, err)
	}

	return &emptypb.Empty{}, nil
}
