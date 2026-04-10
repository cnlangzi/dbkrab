-- Migration: 001_transactions
-- Description: Create transactions and poller_state tables

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    data TEXT,
    changed_at TIMESTAMP,
    pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS poller_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_poll_time TIMESTAMP,
    last_lsn TEXT,
    total_changes INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_table_name ON transactions(table_name);
CREATE INDEX IF NOT EXISTS idx_changed_at ON transactions(changed_at);
