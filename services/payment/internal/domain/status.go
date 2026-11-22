package domain

import (
	"fmt"
	"time"
)

// Status is the lifecycle state of a payment.
type Status string

const (
	StatusCreated    Status = "created"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusReview     Status = "review"
)

// IsTerminal reports whether no further transition is expected from the status.
func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed
}

func (s Status) canTransitionTo(next Status) bool {
	switch s {
	case StatusCreated:
		return next == StatusProcessing
	case StatusProcessing:
		return next == StatusSucceeded || next == StatusFailed || next == StatusReview
	case StatusReview:
		return next == StatusSucceeded || next == StatusFailed
	case StatusSucceeded, StatusFailed:
		return false
	}

	return false
}

// TransitionTo moves the payment to next. A transition to the current status is a no-op
// (changed is false, err is nil) so a retried step emits no second event. Any other
// transition outside the table returns ErrInvalidTransition and leaves the status unchanged.
func (p *Payment) TransitionTo(next Status) (bool, error) {
	if p.Status == next {
		return false, nil
	}

	if !p.Status.canTransitionTo(next) {
		return false, fmt.Errorf("domain.Payment.TransitionTo: %w: %s -> %s", ErrInvalidTransition, p.Status, next)
	}

	p.Status = next
	p.UpdatedAt = time.Now().UTC()

	return true, nil
}
