BEGIN;

CREATE TABLE account (
    id          UUID PRIMARY KEY,
    kind        TEXT        NOT NULL CHECK (kind IN ('customer', 'bank_cash')),
    currency    CHAR(3)     NOT NULL,
    normal_side TEXT        NOT NULL CHECK (normal_side IN ('debit', 'credit')),
    balance     BIGINT      NOT NULL DEFAULT 0,
    held        BIGINT      NOT NULL DEFAULT 0 CHECK (held >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (kind <> 'customer' OR balance - held >= 0)
);
CREATE UNIQUE INDEX account_bank_cash_currency ON account (currency) WHERE kind = 'bank_cash';

CREATE TABLE ledger_transaction (
    id         UUID PRIMARY KEY,
    kind       TEXT        NOT NULL CHECK (kind IN ('deposit', 'payment')),
    reference  TEXT        NOT NULL,
    currency   CHAR(3)     NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (kind, reference)
);

CREATE TABLE ledger_entry (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id UUID        NOT NULL REFERENCES ledger_transaction (id),
    account_id     UUID        NOT NULL REFERENCES account (id),
    side           TEXT        NOT NULL CHECK (side IN ('debit', 'credit')),
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ledger_entry_account ON ledger_entry (account_id);

CREATE TABLE hold (
    payment_id UUID PRIMARY KEY,
    account_id UUID        NOT NULL REFERENCES account (id),
    amount     BIGINT      NOT NULL CHECK (amount > 0),
    currency   CHAR(3)     NOT NULL,
    status     TEXT        NOT NULL CHECK (status IN ('active', 'captured', 'released')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE FUNCTION ledger_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger rows are immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entry_immutable BEFORE UPDATE OR DELETE ON ledger_entry
    FOR EACH ROW EXECUTE FUNCTION ledger_immutable();
CREATE TRIGGER ledger_transaction_immutable BEFORE UPDATE OR DELETE ON ledger_transaction
    FOR EACH ROW EXECUTE FUNCTION ledger_immutable();

COMMIT;
