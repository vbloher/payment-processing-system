//go:build integration

package e2e

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/vbloher/payment-processing-system/pkg/events"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// A payment's outcome yields exactly one notification (spec scenario 1), whether it succeeded or
// the bank declined it.
func (s *Suite) Test_Notification_OnePerPayment() {
	tests := []struct {
		name        string
		beneficiary string
		wantType    events.Type
		wantStatus  string
		wantReason  string // "" means data.failure_reason is absent
	}{
		{name: "succeeded", beneficiary: "DE-OK-1", wantType: events.TypePaymentSucceeded, wantStatus: "succeeded"},
		{name: "bank declined", beneficiary: "DECLINE-1", wantType: events.TypePaymentFailed, wantStatus: "failed",
			wantReason: "bank_declined"},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			account := s.openAccount("EUR", 10000)

			id, code := s.createPayment(uuid.NewString(), account, amount, "EUR", tt.beneficiary)
			s.Equal(http.StatusAccepted, code)

			call := s.waitWebhookCall(id)
			s.Never(func() bool { return len(s.webhookCalls(id)) > 1 }, time.Second, waitTick,
				"a second webhook call for payment %s", id)

			s.Equal(call.Envelope.ID.String(), call.IdempotencyKey)
			s.Equal(tt.wantType, call.Envelope.Type)

			var data map[string]any

			s.NoError(json.Unmarshal(call.Envelope.Data, &data))
			s.Equal(tt.wantStatus, data["status"])

			if tt.wantReason == "" {
				s.NotContains(data, "failure_reason")
			} else {
				s.Equal(tt.wantReason, data["failure_reason"])
			}
		})
	}
}

// Spec section 8: the trace goes from the transaction that saved the event, through the outbox
// and NATS, into the consumer; the outbox worker's own polling is not traced.
func (s *Suite) Test_Tracing_OutboxToConsumer() {
	account := s.openAccount("EUR", 10000)

	id, code := s.createPayment(uuid.NewString(), account, amount, "EUR", "DE-OK-1")
	s.Equal(http.StatusAccepted, code)
	s.waitStatus(id, "succeeded")

	call := s.waitWebhookCall(id)
	s.Equal(events.TypePaymentSucceeded, call.Envelope.Type)

	const consume = "nats consume events.payment.succeeded"

	var consumed sdktrace.ReadOnlySpan

	s.Eventually(func() bool {
		consumed = s.endedSpan(consume, call.Envelope.ID.String())

		return consumed != nil
	}, waitTimeout, waitTick, "no ended span %q for event %s", consume, call.Envelope.ID)

	names := s.spanNames(consumed.SpanContext().TraceID())
	s.Contains(names, "adapter postgres SaveOutbox")
	s.Contains(names, "nats publish events.payment.succeeded")
	s.Contains(names, consume)

	for _, span := range s.spans.Ended() {
		s.NotEqual("usecase SendOutbox", span.Name())
	}
}
