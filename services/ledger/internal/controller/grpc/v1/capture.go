package v1

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (h Handlers) Capture(ctx context.Context, i *pb.CaptureInput) (*emptypb.Empty, error) {
	paymentID, err := parseUUID(ctx, i.GetPaymentId())
	if err != nil {
		return nil, err
	}

	err = h.usecase.Capture(ctx, dto.CaptureInput{PaymentID: paymentID})
	if err != nil {
		return nil, grpcError(ctx, err)
	}

	return &emptypb.Empty{}, nil
}
