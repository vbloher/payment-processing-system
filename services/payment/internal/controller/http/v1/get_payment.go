package v1

import (
	"context"
	"errors"

	http_server "github.com/vbloher/payment-processing-system/gen/payment/http/server"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

func (h *Handlers) GetPayment(
	ctx context.Context, request http_server.GetPaymentRequestObject,
) (http_server.GetPaymentResponseObject, error) {
	output, err := h.usecase.GetPayment(ctx, dto.GetPaymentInput{ID: request.ID})
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return http_server.GetPayment404JSONResponse{NotFoundJSONResponse: notFound(ctx, err)}, nil
		}

		return nil, err
	}

	return http_server.GetPayment200JSONResponse(toPayment(output.Payment)), nil
}
