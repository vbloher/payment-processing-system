package v1

import (
	"context"
	"errors"

	http_server "github.com/vbloher/payment-processing-system/gen/payment/http/server"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

func (h *Handlers) CreatePayment(
	ctx context.Context, request http_server.CreatePaymentRequestObject,
) (http_server.CreatePaymentResponseObject, error) {
	var description string
	if request.Body.Description != nil {
		description = *request.Body.Description
	}

	output, err := h.usecase.CreatePayment(ctx, dto.CreatePaymentInput{
		IdempotencyKey: request.Params.IdempotencyKey,
		AccountID:      request.Body.AccountID,
		Amount:         request.Body.Amount,
		Currency:       request.Body.Currency,
		Beneficiary:    request.Body.Beneficiary,
		Description:    description,
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrIdempotencyConflict):
			return http_server.CreatePayment409JSONResponse{ConflictJSONResponse: conflict(ctx, err)}, nil
		case domain.IsInvalidInput(err):
			return http_server.CreatePayment400JSONResponse{BadRequestJSONResponse: badRequest(ctx, err)}, nil
		default:
			return nil, err
		}
	}

	return http_server.CreatePayment202JSONResponse(toPayment(output.Payment)), nil
}
