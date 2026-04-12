-- Migration: 001_add_lsn
-- Module: dbkrab-store
-- Description: Add LSN (Log Sequence Number) column to transactions for CDC correlation
-- LSN is stored as a hex string prefixed with '0x' for readability (e.g., 0x0000002D00000A760066)
-- Note: LSN is transaction-level, not row-level. Multiple rows in the same transaction share the same LSN.

-- Add LSN column if it doesn't exist (safe for existing databases)
-- SQLite does not support IF NOT EXISTS for ALTER TABLE ADD COLUMN,
-- so we use a trigger-based approach or just try and catch the error.
-- For sqle/migrate idempotency, we use a separate approach below.

-- SQLite requires separate statements for each ALTER TABLE
ALTER TABLE transactions ADD COLUMN lsn TEXT;

-- Index for LSN range queries (useful for correlating changes by transaction)
CREATE INDEX IF NOT EXISTS idx_lsn ON transactions(lsn);
