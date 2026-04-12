-- Migration: 002_add_lsn_to_transactions
-- Description: Add LSN (Log Sequence Number) column to transactions table for CDC correlation
-- LSN is stored as a hex string prefixed with '0x' for readability (e.g., 0x0000002D00000A760066)
-- Note: LSN is transaction-level, not row-level. Multiple rows in the same transaction share the same LSN.

ALTER TABLE transactions ADD COLUMN lsn TEXT;

-- Index for LSN range queries (useful for correlating changes by transaction)
CREATE INDEX IF NOT EXISTS idx_lsn ON transactions(lsn);
