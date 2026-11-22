package events_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/events"
)

func TestType_Subject(t *testing.T) {
	require.Equal(t, "events.payment.succeeded", events.TypePaymentSucceeded.Subject())
	require.Equal(t, "events.reconciliation.discrepancy_found", events.TypeDiscrepancyFound.Subject())
}

func TestEnvelope_RoundTrip(t *testing.T) {
	paymentID := uuid.New()
	e, err := events.New(events.TypePaymentFailed, paymentID, events.PaymentData{Amount: 3000, Currency: "EUR", Status: "failed", FailureReason: "bank_declined"})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, e.ID)
	require.False(t, e.OccurredAt.IsZero())

	raw, err := e.Marshal()
	require.NoError(t, err)

	got, err := events.Unmarshal(raw)
	require.NoError(t, err)
	require.Equal(t, e.ID, got.ID)
	require.Equal(t, paymentID, got.PaymentID)
	require.JSONEq(t, `{"account_id":"00000000-0000-0000-0000-000000000000","amount":3000,"currency":"EUR","status":"failed","failure_reason":"bank_declined"}`, string(got.Data))
}

func TestUnmarshal_Invalid(t *testing.T) {
	for _, raw := range []string{`not json`, `{}`, `{"id":"` + uuid.NewString() + `"}`} {
		_, err := events.Unmarshal([]byte(raw))
		require.ErrorIs(t, err, events.ErrInvalidEnvelope)
	}
}

func TestStreamConfig(t *testing.T) {
	require.Equal(t, []string{"events.>"}, events.StreamConfig().Subjects)
	require.Equal(t, 5*time.Minute, events.StreamConfig().Duplicates)
	require.Equal(t, []string{"dlq.>"}, events.DLQStreamConfig().Subjects)
}
