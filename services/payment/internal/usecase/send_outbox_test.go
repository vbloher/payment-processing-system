package usecase_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/vbloher/payment-processing-system/pkg/otel"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

func Test_SendOutbox_Empty(t *testing.T) {
	otel.SilentModeInit()

	u, d := newUseCase(t)
	d.postgres.EXPECT().ReadOutbox(Any, 10).Return(nil, nil)

	sent, err := u.SendOutbox(t.Context(), 10)
	require.NoError(t, err)
	require.Zero(t, sent)
}

func Test_SendOutbox_Success(t *testing.T) {
	otel.SilentModeInit()

	events := []domain.Event{{ID: uuid.New(), Subject: "s"}, {ID: uuid.New(), Subject: "s"}}

	u, d := newUseCase(t)
	d.postgres.EXPECT().ReadOutbox(Any, 10).Return(events, nil)
	d.publisher.EXPECT().Publish(Any, events).Return(nil)

	sent, err := u.SendOutbox(t.Context(), 10)
	require.NoError(t, err)
	require.Equal(t, len(events), sent)
}

func Test_SendOutbox_ProduceError(t *testing.T) {
	otel.SilentModeInit()

	events := []domain.Event{{ID: uuid.New(), Subject: "s"}}
	errNATS := errors.New("nats is down")

	u, d := newUseCase(t)
	d.postgres.EXPECT().ReadOutbox(Any, 10).Return(events, nil)
	d.publisher.EXPECT().Publish(Any, events).Return(errNATS)

	sent, err := u.SendOutbox(t.Context(), 10)
	require.ErrorIs(t, err, errNATS)
	require.Zero(t, sent)
}
