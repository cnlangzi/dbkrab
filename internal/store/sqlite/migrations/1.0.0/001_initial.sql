-- Migration: 001_initial
-- Module: dbkrab-store
-- Description: Create complete initial schema for dbkrab unified app DB
--   - changes: CDC change events
--   - poller_state: polling progress and metrics
--   - offsets: LSN offsets per table (three-value approach)
--   - dlq_entries: dead letter queue for failed transactions
--
-- Versioning Rules:
--   - Major changes (breaking schema, new tables): increment major, require Devin confirmation
--   - Schema table-structure changes: increment minor version (e.g., 1.1.0, 1.2.0)
--   - Field-only changes (defaults, constraints without structural change): increment patch (e.g., 1.0.1)
--   - All migrations live under the initial semver folder (1.0.0) per current policy decision
--
-- =============================================================================
-- changes: stores CDC change events
-- =============================================================================
-- id is a deterministic content-based hash: SHA256(transaction_id + table_name + data + lsn + operation)
-- truncated to first 16 bytes (32 hex chars). This prevents CDC record loss caused by LSN advancement
-- skipping rows in the same LSN group when using auto-increment primary key.
CREATE TABLE IF NOT EXISTS changes (
    id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    data TEXT,
    lsn TEXT,
    changed_at TIMESTAMP,
    pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_id ON changes(transaction_id);
CREATE INDEX IF NOT EXISTS idx_table_name ON changes(table_name);
CREATE INDEX IF NOT EXISTS idx_changed_at ON changes(changed_at);
CREATE INDEX IF NOT EXISTS idx_lsn ON changes(lsn);

-- =============================================================================
-- poller_state: tracks polling progress and metrics
-- =============================================================================
-- total_changes = total CDC rows fetched from MSSQL
-- total_inserted = total rows actually written to changes table (after INSERT OR IGNORE dedup)
-- Comparing the two reveals duplicate fetches or filtered rows (e.g., UPDATE_BEFORE in all_changes mode)
CREATE TABLE IF NOT EXISTS poller_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_poll_time TIMESTAMP,
    last_lsn TEXT,
    total_changes INTEGER DEFAULT 0,
    total_inserted INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initialize poller state row (idempotent)
INSERT OR IGNORE INTO poller_state (id, last_poll_time, last_lsn, total_changes, total_inserted)
VALUES (1, NULL, NULL, 0, 0);

-- =============================================================================
-- offsets: stores LSN offsets per table using three-value approach
-- =============================================================================
-- Three-value approach reduces remote GetMaxLSN() calls:
--   - last_lsn: the last LSN from fetched data (tracks progress)
--   - next_lsn: incrementLSN(last) - pre-computed next start point for querying
--   - max_lsn: GetMaxLSN() cached at save time for comparison
--
-- GetFromLSN logic:
--   1. If last_lsn is empty → getMinLSN() (cold start)
--   2. If last_lsn < max_lsn → use next_lsn (new data available)
--   3. If last_lsn == max_lsn → re-fetch max and compare:
--      a. If new_max > last_lsn → use next_lsn
--      b. If new_max == last_lsn → no new data
CREATE TABLE IF NOT EXISTS offsets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    last_lsn TEXT NOT NULL,
    next_lsn TEXT NOT NULL,
    max_lsn TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_offsets_table_name ON offsets(table_name);

-- =============================================================================
-- dlq_entries: dead letter queue for failed transactions
-- =============================================================================
CREATE TABLE IF NOT EXISTS dlq_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id TEXT,
    source TEXT,
    lsn TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    change_data TEXT NOT NULL,
    error_message TEXT NOT NULL,
    retry_count INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    resolved_by TEXT,
    resolved_at DATETIME,
    resolved_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_entries(status);
CREATE INDEX IF NOT EXISTS idx_dlq_table ON dlq_entries(table_name);
CREATE INDEX IF NOT EXISTS idx_dlq_created_at ON dlq_entries(created_at);
CREATE INDEX IF NOT EXISTS idx_dlq_lsn ON dlq_entries(lsn);
