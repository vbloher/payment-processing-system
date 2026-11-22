package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/events"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func TestPayment_ToEvent(t *testing.T) {
	p := domain.Payment{
		ID:            uuid.New(),
		AccountID:     uuid.New(),
		Amount:        3000,
		Currency:      "EUR",
		Status:        domain.StatusFailed,
		FailureReason: domain.ReasonBankDeclined,
	}

	ev, err := p.ToEvent(events.TypePaymentFailed)
	require.NoError(t, err)
	require.Equal(t, "events.payment.failed", ev.Subject)
	require.NotEqual(t, uuid.Nil, ev.ID)
	require.Empty(t, ev.Headers)

	env, err := events.Unmarshal(ev.Payload)
	require.NoError(t, err)
	require.Equal(t, ev.ID, env.ID)
	require.Equal(t, p.ID, env.PaymentID)

	var data events.PaymentData
	require.NoError(t, json.Unmarshal(env.Data, &data))
	require.Equal(t, events.PaymentData{
		AccountID:     p.AccountID,
		Amount:        3000,
		Currency:      "EUR",
		Status:        "failed",
		FailureReason: "bank_declined",
	}, data)
}
