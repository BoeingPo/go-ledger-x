CREATE SCHEMA IF NOT EXISTS ledger;

CREATE TABLE IF NOT EXISTS ledger.wallets (
    id         BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL UNIQUE,
    balance    BIGINT      NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Amounts stored as fixed-point integers with scale 10^4.
-- 1.0000 = 10000, 99.9999 = 999999. App layer handles conversion.
CREATE TABLE IF NOT EXISTS ledger.transactions (
    id              BIGSERIAL    PRIMARY KEY,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    from_wallet_id  BIGINT       REFERENCES ledger.wallets(id),
    to_wallet_id    BIGINT       REFERENCES ledger.wallets(id),
    amount          BIGINT       NOT NULL CHECK (amount > 0),
    type            VARCHAR(20)  NOT NULL CHECK (type IN ('credit', 'debit', 'transfer')),
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_idempotency ON ledger.transactions(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_transactions_from_wallet ON ledger.transactions(from_wallet_id);
CREATE INDEX IF NOT EXISTS idx_transactions_to_wallet   ON ledger.transactions(to_wallet_id);
