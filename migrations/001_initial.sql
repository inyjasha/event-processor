CREATE TABLE IF NOT EXISTS public.clicks (
    clid TEXT PRIMARY KEY,
    ad_id BIGINT NOT NULL,
    click_spend NUMERIC NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.payments (
    clid TEXT NOT NULL,
    payout NUMERIC NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    lease_until TIMESTAMPTZ,
    claim_token UUID,
    PRIMARY KEY (clid, ts)
);