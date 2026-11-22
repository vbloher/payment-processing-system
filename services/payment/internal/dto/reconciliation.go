package dto

import (
	"time"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
)

type ReconcileInput struct {
	Now                       time.Time
	Lookback, Lag, StuckAfter time.Duration
}

type ReconcileOutput struct {
	Checked, Found, Resolved, Signaled, Restarted int
}

type GetDiscrepanciesInput struct{ Status domain.DiscrepancyStatus }

type GetDiscrepanciesOutput struct{ Discrepancies []domain.Discrepancy }
