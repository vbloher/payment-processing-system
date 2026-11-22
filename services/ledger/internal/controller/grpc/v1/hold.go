package v1

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (h Handlers) Hold(ctx context.Context, i *pb.HoldInput) (*emptypb.Empty, error) {
	paymentID, err := parseUUID(ctx, i.GetPaymentId())
	if err != nil {
		return nil, err
	}

	accountID, err := parseUUID(ctx, i.GetAccountId())
	if err != nil {
		return nil, err
	}

	err = h.usecase.Hold(ctx, dto.HoldInput{
		PaymentID: paymentID,
		AccountID: accountID,
		Amount:    i.GetAmount(),
		Currency:  i.GetCurrency(),
	})
	if err != nil {
		return nil, grpcError(ctx, err)
	}

	return &emptypb.Empty{}, nil
}
