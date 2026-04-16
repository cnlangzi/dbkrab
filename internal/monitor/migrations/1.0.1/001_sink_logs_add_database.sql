-- Add database field to sink_logs
ALTER TABLE sink_logs ADD COLUMN database TEXT NOT NULL DEFAULT '';
