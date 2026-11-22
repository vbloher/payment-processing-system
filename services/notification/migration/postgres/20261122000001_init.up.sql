BEGIN;

CREATE TABLE inbox (
    event_id    UUID PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE notification (
    id         UUID PRIMARY KEY,
    event_id   UUID        NOT NULL UNIQUE REFERENCES inbox (event_id),
    event_type TEXT        NOT NULL,
    payment_id UUID        NOT NULL,
    payload    JSONB       NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at    TIMESTAMPTZ
);

COMMIT;
