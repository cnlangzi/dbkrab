-- Migration: 003_three_value_lsn
-- Module: dbkrab-store
-- Description: Refactor LSN offset storage to use three-value approach (last_lsn, next_lsn, max_lsn)
-- This reduces redundant GetMaxLSN() calls by storing max_lsn at save time.
--
-- Schema Change:
--   - last_lsn: last LSN from fetched data (renamed from 'lsn')
--   - next_lsn: incrementLSN(last_lsn) - the next start point for queries
--   - max_lsn: GetMaxLSN() result stored at save time
--
-- GetFromLSN Logic:
--   1. If last_lsn is empty → getMinLSN() (cold start)
--   2. If last_lsn < max_lsn → use next_lsn (new data available)
--   3. If last_lsn == max_lsn → re-fetch max_lsn and compare
--
-- Backward Compatibility:
--   - Migrate existing 'lsn' values to 'last_lsn'
--   - 'has_new_data' flag is no longer used but kept for reference

-- Step 1: Add new columns (nullable for backward compatibility)
ALTER TABLE offsets ADD COLUMN last_lsn TEXT;
ALTER TABLE offsets ADD COLUMN next_lsn TEXT;
ALTER TABLE offsets ADD COLUMN max_lsn TEXT;

-- Step 2: Migrate existing data
-- For existing rows, copy 'lsn' to 'last_lsn'
-- Set 'next_lsn' to NULL (will be computed on next poll)
-- Set 'max_lsn' to NULL (will be fetched on next poll)
UPDATE offsets SET last_lsn = lsn WHERE lsn IS NOT NULL AND last_lsn IS NULL;

-- Step 3: Keep 'lsn' and 'has_new_data' for backward compatibility during transition
-- These columns will be deprecated after migration is verified