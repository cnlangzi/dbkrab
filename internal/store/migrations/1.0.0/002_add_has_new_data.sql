-- Migration: 002_add_has_new_data
-- Module: dbkrab-store
-- Description: Add has_new_data column to offsets table for CDC LSN offset management
-- This column tracks whether the last poll cycle found new data at the stored LSN position.

-- Add has_new_data column to offsets table
-- Default 1 (true) for existing rows to maintain backward compatibility:
-- Existing offsets were set after successful data fetch, so hasNewData should be true.
ALTER TABLE offsets ADD COLUMN has_new_data INTEGER NOT NULL DEFAULT 1;
