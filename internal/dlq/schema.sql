-- Schema for the Dead Letter Queue (DLQ)
-- Stores failed CDC changes that couldn't be processed after retries

CREATE TABLE IF NOT EXISTS dlq_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lsn TEXT NOT NULL,                      -- Log Sequence Number of the failed change
    table_name TEXT NOT NULL,               -- Source table name
    operation TEXT NOT NULL,                -- Operation type: INSERT, UPDATE, DELETE
    change_data TEXT NOT NULL,              -- JSON-encoded change data
    error_message TEXT NOT NULL,            -- Error message from the failed processing
    retry_count INTEGER DEFAULT 0,          -- Number of retry attempts
    status TEXT NOT NULL DEFAULT 'pending', -- Status: pending, resolved, ignored, retrying
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    resolved_by TEXT,                       -- Who resolved: 'replay', 'manual', or username
    resolved_at DATETIME,                   -- When resolved
    resolved_note TEXT                      -- Note about resolution
);

-- Index for faster status-based queries
CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_entries(status);

-- Index for faster table-based queries
CREATE INDEX IF NOT EXISTS idx_dlq_table ON dlq_entries(table_name);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_dlq_created_at ON dlq_entries(created_at);

-- Index for LSN lookups
CREATE INDEX IF NOT EXISTS idx_dlq_lsn ON dlq_entries(lsn);