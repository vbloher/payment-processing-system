BEGIN;

CREATE TABLE reconciliation_discrepancy (
    id          UUID PRIMARY KEY,
    kind        TEXT        NOT NULL CHECK (kind IN ('bank_missing', 'bank_unexpected', 'amount_mismatch', 'ledger_mismatch', 'stuck')),
    reference   TEXT        NOT NULL,
    payment_id  UUID,
    details     TEXT        NOT NULL DEFAULT '',
    status      TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX reconciliation_discrepancy_open ON reconciliation_discrepancy (reference, kind) WHERE status = 'open';

COMMIT;
