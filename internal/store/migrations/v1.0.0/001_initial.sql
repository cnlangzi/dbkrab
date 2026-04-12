-- Migration: 001_initial
-- Module: dbkrab-store
-- Description: Create initial schema for unified app DB (transactions, poller_state, offsets, dlq_entries)

-- =============================================================================
-- transactions: stores CDC transaction changes
-- =============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    data TEXT,
    changed_at TIMESTAMP,
    pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_table_name ON transactions(table_name);
CREATE INDEX IF NOT EXISTS idx_changed_at ON transactions(changed_at);

-- =============================================================================
-- poller_state: tracks polling progress and metrics
-- =============================================================================
CREATE TABLE IF NOT EXISTS poller_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_poll_time TIMESTAMP,
    last_lsn TEXT,
    total_changes INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initialize poller state row (idempotent)
INSERT OR IGNORE INTO poller_state (id, last_poll_time, last_lsn, total_changes)
VALUES (1, NULL, NULL, 0);

-- =============================================================================
-- offsets: stores LSN offsets per table (merged from internal/offset/sqlite.go)
-- =============================================================================
CREATE TABLE IF NOT EXISTS offsets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    lsn TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_offsets_table_name ON offsets(table_name);

-- =============================================================================
-- dlq_entries: dead letter queue for failed transactions (merged from internal/dlq)
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
