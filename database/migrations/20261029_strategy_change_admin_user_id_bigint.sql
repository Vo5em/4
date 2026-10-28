-- Preserve strategy change history while widening Telegram user IDs.
ALTER TABLE strategy_change_events
    ALTER COLUMN admin_user_id TYPE BIGINT;
