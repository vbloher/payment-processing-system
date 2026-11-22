package v1

import (
	"context"

	"github.com/google/uuid"

	http_server "github.com/vbloher/payment-processing-system/gen/payment/http/server"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

func (h *Handlers) GetDiscrepancies(
	ctx context.Context, request http_server.GetDiscrepanciesRequestObject,
) (http_server.GetDiscrepanciesResponseObject, error) {
	var status domain.DiscrepancyStatus
	if request.Params.Status != nil {
		status = domain.DiscrepancyStatus(*request.Params.Status)
	}

	output, err := h.usecase.GetDiscrepancies(ctx, dto.GetDiscrepanciesInput{Status: status})
	if err != nil {
		return nil, err
	}

	list := make([]http_server.Discrepancy, 0, len(output.Discrepancies))
	for _, d := range output.Discrepancies {
		list = append(list, toDiscrepancy(d))
	}

	return http_server.GetDiscrepancies200JSONResponse{Discrepancies: list}, nil
}

// toDiscrepancy leaves payment_id out when no payment matches and resolved_at out while the discrepancy is open.
func toDiscrepancy(d domain.Discrepancy) http_server.Discrepancy {
	out := http_server.Discrepancy{
		ID:        d.ID,
		Kind:      http_server.DiscrepancyKind(d.Kind),
		Reference: d.Reference,
		Details:   d.Details,
		Status:    http_server.DiscrepancyStatus(d.Status),
		CreatedAt: d.CreatedAt.UTC(),
	}

	if d.PaymentID != uuid.Nil {
		paymentID := d.PaymentID
		out.PaymentID = &paymentID
	}

	if !d.ResolvedAt.IsZero() {
		resolvedAt := d.ResolvedAt.UTC()
		out.ResolvedAt = &resolvedAt
	}

	return out
}
