-- Add per-pair diagnostic interaction-count parameters. Safe to run repeatedly.
BEGIN;

ALTER TABLE trading_pairs
    ADD COLUMN IF NOT EXISTS historical_level_touch_zone_bps NUMERIC(18, 8) NOT NULL DEFAULT 10.0,
    ADD COLUMN IF NOT EXISTS historical_level_touch_reset_bps NUMERIC(18, 8) NOT NULL DEFAULT 30.0,
    ADD COLUMN IF NOT EXISTS historical_level_touch_min_separation_candles INTEGER NOT NULL DEFAULT 3;

COMMIT;
