package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/vbloher/payment-processing-system/pkg/otel/tracer"
	"github.com/vbloher/payment-processing-system/services/payment/internal/domain"
	"github.com/vbloher/payment-processing-system/services/payment/internal/dto"
)

type findingKey struct {
	reference string
	kind      domain.DiscrepancyKind
}

type finding struct {
	findingKey

	paymentID uuid.UUID // uuid.Nil when no payment exists
	details   string
}

// Reconcile compares the payments in the window with the bank and the ledger. All lookups finish before
// the first write: a failed lookup aborts the run and never becomes a discrepancy.
// The only repairs are restarting a stuck created payment and cancelling a payment in review.
func (u *UseCase) Reconcile(ctx context.Context, in dto.ReconcileInput) (dto.ReconcileOutput, error) {
	ctx, span := tracer.Start(ctx, "usecase Reconcile")
	defer span.End()

	var out dto.ReconcileOutput

	var err error

	out.Restarted, err = u.restartStuckStarts(ctx, in)
	if err != nil {
		return dto.ReconcileOutput{}, err
	}

	out.Signaled, err = u.resolveReviews(ctx)
	if err != nil {
		return dto.ReconcileOutput{}, err
	}

	open, err := u.postgres.ListOpenDiscrepancies(ctx)
	if err != nil {
		return dto.ReconcileOutput{}, fmt.Errorf("postgres.ListOpenDiscrepancies: %w", err)
	}

	candidates, findings, err := u.collectCandidates(ctx, in, open)
	if err != nil {
		return dto.ReconcileOutput{}, err
	}

	out.Checked = len(candidates)

	recheck, err := u.recheckPaymentless(ctx, open, findings)
	if err != nil {
		return dto.ReconcileOutput{}, err
	}

	findings = append(findings, recheck...)

	more, err := u.checkPayments(ctx, in, candidates)
	if err != nil {
		return dto.ReconcileOutput{}, err
	}

	findings = append(findings, more...)

	out.Found, out.Resolved, err = u.applyFindings(ctx, in.Now, open, findings)
	if err != nil {
		return dto.ReconcileOutput{}, err
	}

	return out, nil
}

// restartStuckStarts starts again the workflow of every payment that stayed "created" for too long.
func (u *UseCase) restartStuckStarts(ctx context.Context, in dto.ReconcileInput) (int, error) {
	created, err := u.postgres.ListPaymentsByStatus(ctx, domain.StatusCreated)
	if err != nil {
		return 0, fmt.Errorf("postgres.ListPaymentsByStatus: %w", err)
	}

	restarted := 0

	for _, p := range created {
		if !p.UpdatedAt.Before(in.Now.Add(-in.StuckAfter)) {
			continue
		}

		err = u.orchestrator.StartPayment(ctx, p.ID)
		if err != nil {
			log.Error().Err(err).Str("payment_id", p.ID.String()).Msg("reconcile: restart stuck payment")

			continue
		}

		restarted++
	}

	return restarted, nil
}

// resolveReviews cancels the bank transfer of every payment in review and tells its workflow the outcome.
func (u *UseCase) resolveReviews(ctx context.Context) (int, error) {
	review, err := u.postgres.ListPaymentsByStatus(ctx, domain.StatusReview)
	if err != nil {
		return 0, fmt.Errorf("postgres.ListPaymentsByStatus: %w", err)
	}

	signaled := 0

	for _, p := range review {
		transfer, err := u.bank.Cancel(ctx, p.ID.String())
		if err != nil {
			log.Error().Err(err).Str("payment_id", p.ID.String()).Msg("reconcile: cancel transfer")

			continue
		}

		// The workflow may have finished already: Temporal answers NotFound. That is not a failed run.
		err = u.orchestrator.SignalBankOutcome(ctx, p.ID, transfer.Status)
		if err != nil {
			log.Error().Err(err).Str("payment_id", p.ID.String()).Msg("reconcile: signal bank outcome")

			continue
		}

		signaled++
	}

	return signaled, nil
}

// collectCandidates returns the payments to check and the bank_unexpected findings from the statement.
func (u *UseCase) collectCandidates(
	ctx context.Context, in dto.ReconcileInput, open []domain.Discrepancy,
) ([]domain.Payment, []finding, error) {
	from, to := in.Now.Add(-in.Lookback), in.Now.Add(-in.Lag)

	statement, err := u.bank.GetStatement(ctx, from, in.Now)
	if err != nil {
		return nil, nil, fmt.Errorf("bank.GetStatement: %w", err)
	}

	candidates, err := u.postgres.ListPaymentsUpdatedBetween(ctx, from, to)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres.ListPaymentsUpdatedBetween: %w", err)
	}

	statementRefs, findings := splitStatement(statement)

	wanted := missingIDs(candidates, statementRefs, open)

	if len(wanted) > 0 {
		extra, err := u.postgres.GetPayments(ctx, wanted)
		if err != nil {
			return nil, nil, fmt.Errorf("postgres.GetPayments: %w", err)
		}

		candidates = append(candidates, extra...)
	}

	have := make(map[uuid.UUID]bool, len(candidates))
	for _, p := range candidates {
		have[p.ID] = true
	}

	// A statement reference that is a UUID but names no payment is as unexpected as a non-UUID one.
	for id, ref := range statementRefs {
		if !have[id] {
			findings = append(findings, unexpected(ref))
		}
	}

	return candidates, findings, nil
}

// recheckPaymentless point-looks-up the open bank_unexpected discrepancies that have no payment and are not
// in the current statement: an entry that left the statement window is not an entry that was explained.
func (u *UseCase) recheckPaymentless(
	ctx context.Context, open []domain.Discrepancy, produced []finding,
) ([]finding, error) {
	have := make(map[findingKey]bool, len(produced))
	for _, f := range produced {
		have[f.findingKey] = true
	}

	var findings []finding

	for _, d := range open {
		key := findingKey{d.Reference, d.Kind}
		if d.PaymentID != uuid.Nil || d.Kind != domain.KindBankUnexpected || have[key] {
			continue
		}

		transfer, err := u.bank.GetTransfer(ctx, d.Reference)
		if err != nil {
			return nil, fmt.Errorf("bank.GetTransfer: %w", err)
		}

		if transfer != nil && transfer.Status == domain.BankSucceeded {
			findings = append(findings, unexpected(d.Reference))
		}
	}

	return findings, nil
}

// missingIDs lists the payments that the window did not return but the statement or an open discrepancy names.
func missingIDs(windowed []domain.Payment, statementRefs map[uuid.UUID]string, open []domain.Discrepancy) []uuid.UUID {
	seen := make(map[uuid.UUID]bool, len(windowed))
	for _, p := range windowed {
		seen[p.ID] = true
	}

	var wanted []uuid.UUID

	add := func(id uuid.UUID) {
		if id != uuid.Nil && !seen[id] {
			wanted = append(wanted, id)
			seen[id] = true
		}
	}

	for id := range statementRefs {
		add(id)
	}

	for _, d := range open {
		add(d.PaymentID)
	}

	return wanted
}

// splitStatement separates the references that are payment IDs from the ones that cannot be (bank_unexpected).
func splitStatement(statement []domain.BankTransfer) (map[uuid.UUID]string, []finding) {
	refs := make(map[uuid.UUID]string)

	var findings []finding

	for _, t := range statement {
		id, err := uuid.Parse(t.Reference)
		if err != nil {
			findings = append(findings, unexpected(t.Reference))

			continue
		}

		refs[id] = t.Reference
	}

	return refs, findings
}

func unexpected(reference string) finding {
	return finding{
		reference: reference, kind: domain.KindBankUnexpected,
		paymentID: uuid.Nil,
		details:   "the bank statement has a transfer that matches no payment",
	}
}

// checkPayments looks up the bank and ledger facts of the terminal candidates and applies the rules.
func (u *UseCase) checkPayments(
	ctx context.Context, in dto.ReconcileInput, candidates []domain.Payment,
) ([]finding, error) {
	var terminalIDs []uuid.UUID

	for _, p := range candidates {
		if p.Status.IsTerminal() {
			terminalIDs = append(terminalIDs, p.ID)
		}
	}

	var holds map[uuid.UUID]domain.HoldState

	if len(terminalIDs) > 0 {
		var err error

		holds, err = u.ledger.GetHolds(ctx, terminalIDs)
		if err != nil {
			return nil, fmt.Errorf("ledger.GetHolds: %w", err)
		}
	}

	var findings []finding

	for _, p := range candidates {
		var transfer *domain.BankTransfer

		if p.Status.IsTerminal() {
			var err error

			transfer, err = u.bank.GetTransfer(ctx, p.ID.String())
			if err != nil {
				return nil, fmt.Errorf("bank.GetTransfer: %w", err)
			}
		}

		kinds := domain.Reconcile(domain.ReconcileFacts{
			Payment: p, Bank: transfer, Hold: holds[p.ID], Now: in.Now, StuckAfter: in.StuckAfter,
		})

		for _, k := range kinds {
			findings = append(findings, finding{
				reference: p.ID.String(), kind: k,
				paymentID: p.ID,
				details:   fmt.Sprintf("payment %s: %s", p.Status, k),
			})
		}
	}

	return findings, nil
}

// applyFindings records every new finding with its event and resolves open discrepancies nothing reproduces.
func (u *UseCase) applyFindings(
	ctx context.Context, now time.Time, open []domain.Discrepancy, findings []finding,
) (found, resolved int, err error) {
	openByKey := make(map[findingKey]domain.Discrepancy, len(open))
	for _, d := range open {
		openByKey[findingKey{d.Reference, d.Kind}] = d
	}

	reproduced := make(map[findingKey]bool, len(findings))

	for _, f := range findings {
		if reproduced[f.findingKey] {
			continue
		}

		reproduced[f.findingKey] = true

		if _, exists := openByKey[f.findingKey]; exists {
			continue
		}

		created, recordErr := u.recordDiscrepancy(ctx, now, f)
		if recordErr != nil {
			return found, 0, recordErr
		}

		if created {
			found++
		}
	}

	resolved, err = u.resolveStale(ctx, openByKey, reproduced)

	return found, resolved, err
}

func (u *UseCase) resolveStale(
	ctx context.Context, openByKey map[findingKey]domain.Discrepancy, reproduced map[findingKey]bool,
) (int, error) {
	resolved := 0

	for key, d := range openByKey {
		if reproduced[key] {
			continue
		}

		ok, err := u.resolveDiscrepancy(ctx, d.ID)
		if err != nil {
			return resolved, err
		}

		if ok {
			resolved++
		}
	}

	return resolved, nil
}

// resolveDiscrepancy reports false when a concurrent run resolved the discrepancy first.
func (u *UseCase) resolveDiscrepancy(ctx context.Context, id uuid.UUID) (bool, error) {
	err := u.postgres.ResolveDiscrepancy(ctx, id)

	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, domain.ErrNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("postgres.ResolveDiscrepancy: %w", err)
	}
}

func (u *UseCase) recordDiscrepancy(ctx context.Context, now time.Time, f finding) (bool, error) {
	d := domain.NewDiscrepancy(f.kind, f.reference, f.paymentID, f.details)
	d.CreatedAt = now.UTC()

	created := false

	err := u.tx.Wrap(ctx, func(ctx context.Context) error {
		var err error

		created, err = u.postgres.CreateDiscrepancy(ctx, d)
		if err != nil {
			return fmt.Errorf("postgres.CreateDiscrepancy: %w", err)
		}

		if !created {
			return nil
		}

		event, err := d.ToEvent()
		if err != nil {
			return fmt.Errorf("discrepancy.ToEvent: %w", err)
		}

		err = u.postgres.SaveOutbox(ctx, event)
		if err != nil {
			return fmt.Errorf("postgres.SaveOutbox: %w", err)
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("transaction.Wrap: %w", err)
	}

	return created, nil
}
