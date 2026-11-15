-- Opt-in gate for newly created trade plans using confirmed Trendln phases.
-- Existing rows default to disabled to preserve their current behavior.
ALTER TABLE trading_pairs
    ADD COLUMN IF NOT EXISTS historical_level_active_filter_enabled BOOLEAN NOT NULL DEFAULT false;
