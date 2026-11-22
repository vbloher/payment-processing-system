package v1

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vbloher/payment-processing-system/pkg/render"
	"github.com/vbloher/payment-processing-system/services/ledger/internal/domain"
)

// errBadUUID marks an identifier that is not a UUID.
var errBadUUID = errors.New("invalid uuid")

func parseUUID(ctx context.Context, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, grpcError(ctx, errors.Join(errBadUUID, err))
	}

	return id, nil
}

func grpcError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, render.Error(ctx, err, "request failed").Error())
	case domain.IsInvalidInput(err), errors.Is(err, errBadUUID):
		return status.Error(codes.InvalidArgument, render.Error(ctx, err, "request failed").Error())
	case errors.Is(err, domain.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, render.Error(ctx, err, "request failed").Error())
	case errors.Is(err, domain.ErrHoldConflict):
		return status.Error(codes.Aborted, render.Error(ctx, err, "request failed").Error())
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, render.Error(ctx, err, "request failed").Error())
	default:
		_ = render.Error(ctx, err, "internal error")

		return status.Error(codes.Internal, http.StatusText(http.StatusInternalServerError))
	}
}
