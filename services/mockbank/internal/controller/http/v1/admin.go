package v1

import (
	"context"
	"errors"

	http_server "github.com/vbloher/payment-processing-system/gen/mockbank/http/server"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/dto"
)

func (h *Handlers) SetAvailability(
	ctx context.Context, request http_server.SetAvailabilityRequestObject,
) (http_server.SetAvailabilityResponseObject, error) {
	h.usecase.SetAvailability(ctx, request.Body.Available)

	return http_server.SetAvailability204Response{}, nil
}

func (h *Handlers) AddTransfer(
	ctx context.Context, request http_server.AddTransferRequestObject,
) (http_server.AddTransferResponseObject, error) {
	t, err := h.usecase.AddTransfer(ctx, dto.CreateTransferInput{
		Reference:   request.Body.Reference,
		Amount:      request.Body.Amount,
		Currency:    request.Body.Currency,
		Beneficiary: request.Body.Beneficiary,
	})
	if err != nil {
		return nil, err
	}

	return http_server.AddTransfer201JSONResponse(toTransfer(t)), nil
}

func (h *Handlers) SetTransferAmount(
	ctx context.Context, request http_server.SetTransferAmountRequestObject,
) (http_server.SetTransferAmountResponseObject, error) {
	t, err := h.usecase.SetTransferAmount(ctx, request.Reference, request.Body.Amount)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return http_server.SetTransferAmount404JSONResponse{NotFoundJSONResponse: notFound(ctx, err)}, nil
		}

		return nil, err
	}

	return http_server.SetTransferAmount200JSONResponse{TransferOKJSONResponse: transferOK(t)}, nil
}
