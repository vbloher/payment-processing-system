package v1

import (
	"context"
	"errors"

	http_server "github.com/vbloher/payment-processing-system/gen/mockbank/http/server"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
	"github.com/vbloher/payment-processing-system/services/mockbank/internal/dto"
)

func toTransfer(t domain.Transfer) http_server.Transfer {
	res := http_server.Transfer{
		ID:          t.ID,
		Reference:   t.Reference,
		Amount:      t.Amount,
		Currency:    t.Currency,
		Beneficiary: t.Beneficiary,
		Status:      http_server.TransferStatus(t.Status),
		CreatedAt:   t.CreatedAt,
	}

	if t.DeclineReason != "" {
		res.DeclineReason = &t.DeclineReason
	}

	return res
}

func (h *Handlers) CreateTransfer(
	ctx context.Context, request http_server.CreateTransferRequestObject,
) (http_server.CreateTransferResponseObject, error) {
	t, err := h.usecase.CreateTransfer(ctx, dto.CreateTransferInput{
		Reference:   request.Params.IdempotencyKey,
		Amount:      request.Body.Amount,
		Currency:    request.Body.Currency,
		Beneficiary: request.Body.Beneficiary,
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrUnavailable):
			return http_server.CreateTransfer503JSONResponse{UnavailableJSONResponse: unavailable(ctx, err)}, nil
		case errors.Is(err, domain.ErrTimeout):
			return http_server.CreateTransfer504JSONResponse{TimeoutJSONResponse: timeout(ctx, err)}, nil
		case errors.Is(err, domain.ErrConflict):
			return http_server.CreateTransfer409JSONResponse{ConflictJSONResponse: conflict(ctx, err)}, nil
		case errors.Is(err, domain.ErrValidation):
			return http_server.CreateTransfer400JSONResponse{BadRequestJSONResponse: badRequest(ctx, err)}, nil
		default:
			return nil, err
		}
	}

	return http_server.CreateTransfer200JSONResponse{TransferOKJSONResponse: transferOK(t)}, nil
}

func (h *Handlers) CancelTransfer(
	ctx context.Context, request http_server.CancelTransferRequestObject,
) (http_server.CancelTransferResponseObject, error) {
	t, err := h.usecase.CancelTransfer(ctx, request.Reference)
	if err != nil {
		if errors.Is(err, domain.ErrUnavailable) {
			return http_server.CancelTransfer503JSONResponse{UnavailableJSONResponse: unavailable(ctx, err)}, nil
		}

		return nil, err
	}

	return http_server.CancelTransfer200JSONResponse{TransferOKJSONResponse: transferOK(t)}, nil
}

func (h *Handlers) GetTransfer(
	ctx context.Context, request http_server.GetTransferRequestObject,
) (http_server.GetTransferResponseObject, error) {
	t, err := h.usecase.GetTransfer(ctx, request.Reference)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrNotFound):
			return http_server.GetTransfer404JSONResponse{NotFoundJSONResponse: notFound(ctx, err)}, nil
		case errors.Is(err, domain.ErrUnavailable):
			return http_server.GetTransfer503JSONResponse{UnavailableJSONResponse: unavailable(ctx, err)}, nil
		default:
			return nil, err
		}
	}

	return http_server.GetTransfer200JSONResponse{TransferOKJSONResponse: transferOK(t)}, nil
}

func (h *Handlers) GetStatement(
	ctx context.Context, request http_server.GetStatementRequestObject,
) (http_server.GetStatementResponseObject, error) {
	transfers, err := h.usecase.GetStatement(ctx, request.Params.From.UTC(), request.Params.To.UTC())
	if err != nil {
		if errors.Is(err, domain.ErrUnavailable) {
			return http_server.GetStatement503JSONResponse{UnavailableJSONResponse: unavailable(ctx, err)}, nil
		}

		return nil, err
	}

	res := make([]http_server.Transfer, 0, len(transfers))
	for _, t := range transfers {
		res = append(res, toTransfer(t))
	}

	return http_server.GetStatement200JSONResponse{Transfers: res}, nil
}

func transferOK(t domain.Transfer) http_server.TransferOKJSONResponse {
	return http_server.TransferOKJSONResponse(toTransfer(t))
}
