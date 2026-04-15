-- Migration: 001_initial
-- Module: dbkrab-offset
-- Description: Create offsets table for LSN offset storage per table
--   - offsets: LSN offsets per table (three-value approach)
--
-- Versioning Rules:
--   - Major changes (breaking schema, new tables): increment major, require Devin confirmation
--   - Schema table-structure changes: increment minor version (e.g., 1.1.0, 1.2.0)
--   - Field-only changes (defaults, constraints without structural change): increment patch (e.g., 1.0.1)
--   - All migrations live under the initial semver folder (1.0.0) per current policy decision
--
-- =============================================================================
-- offsets: stores LSN offsets per table
-- =============================================================================
-- last_lsn: the last LSN from fetched data (tracks progress)
-- next_lsn: incrementLSN(last) - pre-computed next start point for querying
--
-- GetFromLSN logic (uses globalMaxLSN from poll start, not stored max_lsn):
--   1. If last_lsn is empty → getMinLSN() (cold start)
--   2. If last_lsn != globalMaxLSN → new data available, use next_lsn as fromLSN
--   3. If last_lsn == globalMaxLSN → no new data
CREATE TABLE IF NOT EXISTS offsets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    last_lsn TEXT NOT NULL,
    next_lsn TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_offsets_table_name ON offsets(table_name);