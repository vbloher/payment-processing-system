package v1

import (
	"context"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"

	nethttpmiddleware "github.com/oapi-codegen/nethttp-middleware"

	http_server "github.com/vbloher/payment-processing-system/gen/mockbank/http/server"
	"github.com/vbloher/payment-processing-system/pkg/render"
)

func errorResponse(ctx context.Context, err error) http_server.ErrorResponse {
	return http_server.ErrorResponse{Error: render.Error(ctx, err, "request failed").Error()}
}

func badRequest(ctx context.Context, err error) http_server.BadRequestJSONResponse {
	return http_server.BadRequestJSONResponse(errorResponse(ctx, err))
}

func notFound(ctx context.Context, err error) http_server.NotFoundJSONResponse {
	return http_server.NotFoundJSONResponse(errorResponse(ctx, err))
}

func conflict(ctx context.Context, err error) http_server.ConflictJSONResponse {
	return http_server.ConflictJSONResponse(errorResponse(ctx, err))
}

func unavailable(ctx context.Context, err error) http_server.UnavailableJSONResponse {
	return http_server.UnavailableJSONResponse(errorResponse(ctx, err))
}

func timeout(ctx context.Context, err error) http_server.TimeoutJSONResponse {
	return http_server.TimeoutJSONResponse(errorResponse(ctx, err))
}

func RequestErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	invalidRequest(r.Context(), w, err, http.StatusBadRequest)
}

func ResponseErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	render.InternalErrorJSON(r.Context(), w, err)
}

func ValidationErrorHandler(
	ctx context.Context, err error, w http.ResponseWriter, _ *http.Request, opts nethttpmiddleware.ErrorHandlerOpts,
) {
	invalidRequest(ctx, w, err, opts.StatusCode)
}

func SchemaErrorMessage(err *openapi3.SchemaError) string {
	if pointer := err.JSONPointer(); len(pointer) > 0 {
		return "/" + strings.Join(pointer, "/") + ": " + err.Reason
	}

	return err.Reason
}

func invalidRequest(ctx context.Context, w http.ResponseWriter, err error, status int) {
	_ = render.Error(ctx, err, "invalid request")

	render.JSON(w, render.Err{Error: "invalid request: " + err.Error()}, status)
}
