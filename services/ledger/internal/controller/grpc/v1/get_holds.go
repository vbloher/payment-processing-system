package v1

import (
	"context"

	"github.com/google/uuid"

	pb "github.com/vbloher/payment-processing-system/gen/ledger/grpc/ledger_v1"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/dto"
)

func (h Handlers) GetHolds(ctx context.Context, i *pb.GetHoldsInput) (*pb.GetHoldsOutput, error) {
	paymentIDs := make([]uuid.UUID, 0, len(i.GetPaymentIds()))

	for _, raw := range i.GetPaymentIds() {
		id, err := parseUUID(ctx, raw)
		if err != nil {
			return nil, err
		}

		paymentIDs = append(paymentIDs, id)
	}

	output, err := h.usecase.GetHolds(ctx, dto.GetHoldsInput{PaymentIDs: paymentIDs})
	if err != nil {
		return nil, grpcError(ctx, err)
	}

	holds := make([]*pb.GetHoldsOutput_Hold, 0, len(output.Holds))
	for _, hold := range output.Holds {
		holds = append(holds, &pb.GetHoldsOutput_Hold{
			PaymentId: hold.PaymentID.String(),
			AccountId: hold.AccountID.String(),
			Amount:    hold.Amount,
			Currency:  hold.Currency,
			Status:    string(hold.Status),
		})
	}

	return &pb.GetHoldsOutput{Holds: holds}, nil
}
