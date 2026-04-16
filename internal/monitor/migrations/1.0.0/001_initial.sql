-- Migration: Initialize monitor tables
-- Module: dbkrab-monitor
-- Version: 1.0.0
-- Description: Creates batch_logs, skill_logs, and sink_logs tables for observability

-- Batch logs: tracks each batch cycle with summary metrics
CREATE TABLE IF NOT EXISTS batch_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT UNIQUE NOT NULL,           -- UUID with short timestamp (root trace ID)
    fetched_rows INTEGER NOT NULL DEFAULT 0, -- Total CDC rows fetched in this cycle
    tx_count INTEGER NOT NULL DEFAULT 0,     -- Number of transactions processed
    dlq_count INTEGER NOT NULL DEFAULT 0,    -- Number of transactions sent to DLQ
    duration_ms INTEGER NOT NULL DEFAULT 0,  -- Total pull cycle duration in milliseconds
    status TEXT NOT NULL,                    -- SUCCESS / PARTIAL / FAILED
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Skill logs: tracks skill execution per skill per operation
CREATE TABLE IF NOT EXISTS skill_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,                   -- Links to batch_logs.batch_id
    skill_id TEXT NOT NULL,                  -- Skill hash ID (12-char SHA256)
    skill_name TEXT NOT NULL,                -- Skill name from YAML
    operation TEXT NOT NULL,                 -- INSERT / UPDATE / DELETE
    rows_processed INTEGER NOT NULL DEFAULT 0, -- Rows processed by this skill
    status TEXT NOT NULL,                    -- SKIP / EXECUTED / ERROR
    error_message TEXT,                      -- Error details if status=ERROR
    duration_ms INTEGER NOT NULL DEFAULT 0,  -- Skill execution duration in milliseconds
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Sink logs: tracks sink writes per sink per table per operation
CREATE TABLE IF NOT EXISTS sink_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,                   -- Links to batch_logs.batch_id
    skill_name TEXT NOT NULL,                -- Skill that produced this sink
    sink_name TEXT NOT NULL,                 -- Sink config name (database name)
    output_table TEXT NOT NULL,              -- Target table name
    operation TEXT NOT NULL,                 -- INSERT / UPDATE / DELETE
    rows_written INTEGER NOT NULL DEFAULT 0, -- Rows written to sink
    status TEXT NOT NULL,                    -- SUCCESS / ERROR
    error_message TEXT,                      -- Error details if status=ERROR
    duration_ms INTEGER NOT NULL DEFAULT 0,  -- Sink write duration in milliseconds
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_batch_logs_created_at ON batch_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_batch_logs_status ON batch_logs(status);

CREATE INDEX IF NOT EXISTS idx_skill_logs_batch_id ON skill_logs(batch_id);
CREATE INDEX IF NOT EXISTS idx_skill_logs_skill_id ON skill_logs(skill_id);
CREATE INDEX IF NOT EXISTS idx_skill_logs_status ON skill_logs(status);

CREATE INDEX IF NOT EXISTS idx_sink_logs_batch_id ON sink_logs(batch_id);
CREATE INDEX IF NOT EXISTS idx_sink_logs_sink_name ON sink_logs(sink_name);
CREATE INDEX IF NOT EXISTS idx_sink_logs_status ON sink_logs(status);