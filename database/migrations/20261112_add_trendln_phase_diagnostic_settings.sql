-- Add per-pair diagnostic-only Trendln phase-confirmation settings.
-- Idempotent and additive; does not alter trading or history tables.
BEGIN;

ALTER TABLE trading_pairs
    ADD COLUMN IF NOT EXISTS historical_level_break_confirm_bps NUMERIC(18, 8) NOT NULL DEFAULT 20.0,
    ADD COLUMN IF NOT EXISTS historical_level_break_confirm_candles INTEGER NOT NULL DEFAULT 2;

COMMIT;
