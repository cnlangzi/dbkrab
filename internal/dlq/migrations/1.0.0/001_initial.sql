-- Migration: 001_initial
-- Module: dbkrab-dlq
-- Description: Create dlq_entries table for dead letter queue

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
