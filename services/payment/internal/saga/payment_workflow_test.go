package saga_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/saga"
)

var errBoom = errors.New("boom")

type PaymentWorkflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env    *testsuite.TestWorkflowEnvironment
	mocked map[string]bool
}

func TestPaymentWorkflowSuite(t *testing.T) {
	suite.Run(t, new(PaymentWorkflowSuite))
}

func (s *PaymentWorkflowSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.mocked = map[string]bool{}
	s.registerStubs()
}

func (s *PaymentWorkflowSuite) registerStubs() {
	// Every activity must be mocked with OnActivity so that calls are counted; a stub that
	// runs means an unmocked activity was invoked and its call would go uncounted.
	reg := func(fn any, name string) {
		s.env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}

	reg(func(context.Context, uuid.UUID) error { return s.unmocked() }, saga.ActivityStartProcessing)
	reg(func(context.Context, uuid.UUID) (domain.FailureReason, error) { return "", s.unmocked() }, saga.ActivityHoldFunds)
	reg(func(context.Context, uuid.UUID) (domain.BankOutcome, error) { return "", s.unmocked() }, saga.ActivitySendTransfer)
	reg(func(context.Context, uuid.UUID) (domain.BankOutcome, error) { return "", s.unmocked() }, saga.ActivityCancelTransfer)
	reg(func(context.Context, uuid.UUID) error { return s.unmocked() }, saga.ActivityCaptureFunds)
	reg(func(context.Context, uuid.UUID) error { return s.unmocked() }, saga.ActivityReleaseFunds)
	reg(func(context.Context, uuid.UUID) error { return s.unmocked() }, saga.ActivityCompletePayment)
	reg(func(context.Context, saga.FailPaymentInput) error { return s.unmocked() }, saga.ActivityFailPayment)
	reg(func(context.Context, uuid.UUID) error { return s.unmocked() }, saga.ActivityMarkPaymentForReview)
}

func (s *PaymentWorkflowSuite) unmocked() error {
	s.T().Error("an activity without an OnActivity expectation was invoked")

	return errBoom
}

// on registers an expectation and remembers that the activity is mocked.
func (s *PaymentWorkflowSuite) on(name string, args ...any) *testsuite.MockCallWrapper {
	s.mocked[name] = true

	return s.env.OnActivity(name, args...)
}

// mockRest mocks every activity the test did not mock, with a harmless default, so that an
// unexpected call is counted and a "must not run" assertion is real.
func (s *PaymentWorkflowSuite) mockRest() {
	defaults := map[string][]any{
		saga.ActivityStartProcessing:      {nil},
		saga.ActivityHoldFunds:            {domain.ReasonNone, nil},
		saga.ActivitySendTransfer:         {domain.BankOutcome(""), errBoom},
		saga.ActivityCancelTransfer:       {domain.BankOutcome(""), errBoom},
		saga.ActivityCaptureFunds:         {nil},
		saga.ActivityReleaseFunds:         {nil},
		saga.ActivityCompletePayment:      {nil},
		saga.ActivityFailPayment:          {nil},
		saga.ActivityMarkPaymentForReview: {nil},
	}

	for name, ret := range defaults {
		if !s.mocked[name] {
			s.on(name, mock.Anything, mock.Anything).Return(ret...)
		}
	}
}

func (s *PaymentWorkflowSuite) input(id uuid.UUID) saga.PaymentInput {
	return saga.PaymentInput{
		PaymentID:            id,
		ActivityTimeout:      time.Second,
		RetryInitialInterval: time.Millisecond,
		RetryMaxInterval:     10 * time.Millisecond,
		BankMaxAttempts:      3,
	}
}

// bankStep describes what a bank activity returns: an outcome or an unknown-outcome error.
type bankStep struct {
	outcome domain.BankOutcome
	err     error
}

func (s *PaymentWorkflowSuite) onBank(name string, step *bankStep) {
	if step == nil {
		return // mockRest registers a counted default
	}

	s.on(name, mock.Anything, mock.Anything).Return(step.outcome, step.err)
}

func (s *PaymentWorkflowSuite) Test_PaymentWorkflow_Table() {
	unknown := &bankStep{err: errBoom}
	outcome := func(o domain.BankOutcome) *bankStep { return &bankStep{outcome: o} }

	tests := []struct {
		name         string
		hold         domain.FailureReason
		send, cancel *bankStep
		badSignals   []domain.BankOutcome // sent before signal; the workflow must ignore them
		signal       domain.BankOutcome
		wantStatus   domain.Status
		wantReason   domain.FailureReason // expected FailPayment reason, "" = FailPayment must not run
		must         []string
		mustNot      []string
		calls        map[string]int
		captureFlaky bool
	}{
		{name: "1 hold ok, bank succeeded", send: outcome(domain.BankSucceeded), wantStatus: domain.StatusSucceeded,
			must:    []string{saga.ActivityCaptureFunds, saga.ActivityCompletePayment},
			mustNot: []string{saga.ActivityReleaseFunds, saga.ActivityFailPayment, saga.ActivityCancelTransfer}},
		{name: "2 insufficient funds", hold: domain.ReasonInsufficientFunds, wantStatus: domain.StatusFailed,
			wantReason: domain.ReasonInsufficientFunds,
			mustNot:    []string{saga.ActivitySendTransfer, saga.ActivityReleaseFunds}},
		{name: "3 account not found", hold: domain.ReasonAccountNotFound, wantStatus: domain.StatusFailed,
			wantReason: domain.ReasonAccountNotFound, mustNot: []string{saga.ActivitySendTransfer}},
		{name: "4 ledger rejected", hold: domain.ReasonLedgerRejected, wantStatus: domain.StatusFailed,
			wantReason: domain.ReasonLedgerRejected, mustNot: []string{saga.ActivitySendTransfer}},
		{name: "5 bank declined", send: outcome(domain.BankDeclined), wantStatus: domain.StatusFailed,
			wantReason: domain.ReasonBankDeclined,
			must:       []string{saga.ActivityReleaseFunds},
			mustNot:    []string{saga.ActivityCaptureFunds, saga.ActivityCancelTransfer}},
		{name: "6 send unknown, cancel succeeded", send: unknown, cancel: outcome(domain.BankSucceeded),
			wantStatus: domain.StatusSucceeded,
			must:       []string{saga.ActivityCaptureFunds, saga.ActivityCompletePayment},
			mustNot:    []string{saga.ActivityReleaseFunds},
			calls:      map[string]int{saga.ActivitySendTransfer: 3}},
		{name: "7 send unknown, cancel cancelled", send: unknown, cancel: outcome(domain.BankCancelled),
			wantStatus: domain.StatusFailed, wantReason: domain.ReasonBankCancelled,
			must: []string{saga.ActivityReleaseFunds}, mustNot: []string{saga.ActivityCaptureFunds}},
		{name: "8 send unknown, cancel declined", send: unknown, cancel: outcome(domain.BankDeclined),
			wantStatus: domain.StatusFailed, wantReason: domain.ReasonBankDeclined,
			must: []string{saga.ActivityReleaseFunds}, mustNot: []string{saga.ActivityCaptureFunds}},
		{name: "9 both unknown, signal succeeded", send: unknown, cancel: unknown, signal: domain.BankSucceeded,
			wantStatus: domain.StatusSucceeded,
			must: []string{saga.ActivityMarkPaymentForReview, saga.ActivityCaptureFunds,
				saga.ActivityCompletePayment},
			mustNot: []string{saga.ActivityReleaseFunds},
			calls:   map[string]int{saga.ActivityCancelTransfer: 3}},
		{name: "10 both unknown, signal cancelled", send: unknown, cancel: unknown, signal: domain.BankCancelled,
			wantStatus: domain.StatusFailed, wantReason: domain.ReasonBankCancelled,
			must:    []string{saga.ActivityMarkPaymentForReview, saga.ActivityReleaseFunds},
			mustNot: []string{saga.ActivityCaptureFunds}},
		{name: "11 both unknown, signal declined", send: unknown, cancel: unknown, signal: domain.BankDeclined,
			wantStatus: domain.StatusFailed, wantReason: domain.ReasonBankDeclined,
			must:    []string{saga.ActivityMarkPaymentForReview, saga.ActivityReleaseFunds},
			mustNot: []string{saga.ActivityCaptureFunds}},
		{name: "12 both unknown, invalid signals ignored, then signal cancelled", send: unknown, cancel: unknown,
			badSignals: []domain.BankOutcome{"succeded", ""}, signal: domain.BankCancelled,
			wantStatus: domain.StatusFailed, wantReason: domain.ReasonBankCancelled,
			must:    []string{saga.ActivityMarkPaymentForReview, saga.ActivityReleaseFunds},
			mustNot: []string{saga.ActivityCaptureFunds, saga.ActivityCompletePayment}},
		{name: "13 capture fails twice then succeeds", send: outcome(domain.BankSucceeded), captureFlaky: true,
			wantStatus: domain.StatusSucceeded,
			must:       []string{saga.ActivityCompletePayment},
			mustNot:    []string{saga.ActivityReleaseFunds},
			calls:      map[string]int{saga.ActivityCaptureFunds: 3}},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.SetupTest() // fresh environment per row

			id := uuid.New()

			s.on(saga.ActivityStartProcessing, mock.Anything, id).Return(nil)
			s.on(saga.ActivityHoldFunds, mock.Anything, id).Return(tt.hold, nil)
			s.onBank(saga.ActivitySendTransfer, tt.send)
			s.onBank(saga.ActivityCancelTransfer, tt.cancel)
			s.on(saga.ActivityMarkPaymentForReview, mock.Anything, id).Return(nil)
			s.on(saga.ActivityReleaseFunds, mock.Anything, id).Return(nil)
			s.on(saga.ActivityCompletePayment, mock.Anything, id).Return(nil)

			if tt.captureFlaky {
				s.on(saga.ActivityCaptureFunds, mock.Anything, id).Return(errBoom).Twice()
				s.on(saga.ActivityCaptureFunds, mock.Anything, id).Return(nil).Once()
			} else {
				s.on(saga.ActivityCaptureFunds, mock.Anything, id).Return(nil)
			}

			if tt.wantReason != domain.ReasonNone {
				s.on(saga.ActivityFailPayment, mock.Anything,
					saga.FailPaymentInput{PaymentID: id, Reason: tt.wantReason}).Return(nil)
			} else {
				s.on(saga.ActivityFailPayment, mock.Anything, mock.Anything).Return(nil)
			}

			for i, bad := range tt.badSignals {
				s.env.RegisterDelayedCallback(func() {
					s.env.SignalWorkflow(saga.BankOutcomeSignal, bad)
				}, time.Duration(i+1)*time.Minute)
			}

			if tt.signal != "" {
				s.env.RegisterDelayedCallback(func() {
					s.env.SignalWorkflow(saga.BankOutcomeSignal, tt.signal)
				}, time.Duration(len(tt.badSignals)+1)*time.Minute)
			}

			s.mockRest()
			s.env.ExecuteWorkflow(saga.PaymentWorkflow, s.input(id))

			require.True(s.T(), s.env.IsWorkflowCompleted())
			require.NoError(s.T(), s.env.GetWorkflowError())

			var got domain.Status
			require.NoError(s.T(), s.env.GetWorkflowResult(&got))
			require.Equal(s.T(), tt.wantStatus, got)

			for _, name := range tt.must {
				s.env.AssertActivityNumberOfCalls(s.T(), name, 1)
			}

			if tt.wantReason != domain.ReasonNone {
				s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivityFailPayment, 1)
			}

			for _, name := range tt.mustNot {
				s.env.AssertActivityNumberOfCalls(s.T(), name, 0)
			}

			for name, n := range tt.calls {
				s.env.AssertActivityNumberOfCalls(s.T(), name, n)
			}
		})
	}
}

func (s *PaymentWorkflowSuite) Test_PaymentWorkflow_FatalStopsTheWorkflow() {
	id := uuid.New()
	fatal := temporal.NewNonRetryableApplicationError("x", saga.ErrTypeFatal, nil)

	s.on(saga.ActivityStartProcessing, mock.Anything, id).Return(fatal)
	s.on(saga.ActivityHoldFunds, mock.Anything, id).Return(domain.ReasonNone, nil)

	s.mockRest()
	s.env.ExecuteWorkflow(saga.PaymentWorkflow, s.input(id))

	require.True(s.T(), s.env.IsWorkflowCompleted())
	require.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivityStartProcessing, 1)
	s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivityHoldFunds, 0)
}

func (s *PaymentWorkflowSuite) Test_PaymentWorkflow_FatalFromBankIsNotTreatedAsUnknown() {
	id := uuid.New()
	fatal := temporal.NewNonRetryableApplicationError("x", saga.ErrTypeFatal, nil)

	s.on(saga.ActivityStartProcessing, mock.Anything, id).Return(nil)
	s.on(saga.ActivityHoldFunds, mock.Anything, id).Return(domain.ReasonNone, nil)
	s.on(saga.ActivitySendTransfer, mock.Anything, id).Return(domain.BankOutcome(""), fatal)

	s.mockRest()
	s.env.ExecuteWorkflow(saga.PaymentWorkflow, s.input(id))

	require.True(s.T(), s.env.IsWorkflowCompleted())
	require.Error(s.T(), s.env.GetWorkflowError())
	s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivitySendTransfer, 1)
	s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivityCancelTransfer, 0)
	s.env.AssertActivityNumberOfCalls(s.T(), saga.ActivityReleaseFunds, 0)
}

func (s *PaymentWorkflowSuite) Test_PaymentWorkflow_UnexpectedOutcomeNeitherCompensatesNorCompletes() {
	id := uuid.New()

	s.on(saga.ActivityStartProcessing, mock.Anything, id).Return(nil)
	s.on(saga.ActivityHoldFunds, mock.Anything, id).Return(domain.ReasonNone, nil)
	s.on(saga.ActivitySendTransfer, mock.Anything, id).Return(domain.BankOutcome("weird"), nil)

	s.mockRest()
	s.env.ExecuteWorkflow(saga.PaymentWorkflow, s.input(id))

	require.True(s.T(), s.env.IsWorkflowCompleted())
	require.Error(s.T(), s.env.GetWorkflowError())

	for _, name := range []string{saga.ActivityReleaseFunds, saga.ActivityFailPayment,
		saga.ActivityCaptureFunds, saga.ActivityCompletePayment} {
		s.env.AssertActivityNumberOfCalls(s.T(), name, 0)
	}
}
