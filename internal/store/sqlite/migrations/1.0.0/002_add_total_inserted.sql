-- Migration: 002_add_total_inserted
-- Description: Add total_inserted to poller_state for observability
--   total_changes = total CDC rows fetched from MSSQL
--   total_inserted = total rows actually written to changes table (after INSERT OR IGNORE dedup)
--   Comparing the two reveals duplicate fetches or filtered rows (e.g., UPDATE_BEFORE in all_changes mode)

ALTER TABLE poller_state ADD COLUMN total_inserted INTEGER DEFAULT 0;

UPDATE poller_state SET total_inserted = 0 WHERE id = 1;
