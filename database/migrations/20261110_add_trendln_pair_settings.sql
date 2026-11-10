-- Add per-pair Trendln parameters without replacing trading_pairs or modifying
-- existing values. Safe to run more than once on PostgreSQL.
BEGIN;

ALTER TABLE trading_pairs
    ADD COLUMN IF NOT EXISTS historical_level_pctbound NUMERIC(18, 10) NOT NULL DEFAULT 0.05,
    ADD COLUMN IF NOT EXISTS historical_level_min_touches INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS historical_level_extmethod VARCHAR(32) NOT NULL DEFAULT 'METHOD_NUMDIFF',
    ADD COLUMN IF NOT EXISTS historical_level_accuracy INTEGER NOT NULL DEFAULT 2;

COMMIT;
