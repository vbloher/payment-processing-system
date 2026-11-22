BEGIN;

CREATE TABLE payment (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT        NOT NULL UNIQUE,
    request_hash    TEXT        NOT NULL,
    account_id      UUID        NOT NULL,
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    currency        CHAR(3)     NOT NULL,
    beneficiary     TEXT        NOT NULL,
    description     TEXT        NOT NULL DEFAULT '',
    status          TEXT        NOT NULL CHECK (status IN ('created', 'processing', 'succeeded', 'failed', 'review')),
    failure_reason  TEXT        NOT NULL DEFAULT '',
    bank_reference  TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX payment_updated_at ON payment (updated_at);
CREATE INDEX payment_open_status ON payment (status) WHERE status IN ('created', 'processing', 'review');

CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_id   UUID        NOT NULL UNIQUE,
    subject    TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    headers    JSONB       NOT NULL DEFAULT '{}'
);

COMMIT;
