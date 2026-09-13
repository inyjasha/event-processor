CREATE INDEX IF NOT EXISTS payments_pending_queue_idx
ON public.payments (next_attempt_at, clid, ts)
WHERE delivered_at IS NULL;