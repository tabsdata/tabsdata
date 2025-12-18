--
--  Copyright 2025 Tabs Data Inc.
--

-- Add enabled column to functions
ALTER TABLE functions
    ADD COLUMN enabled BOOLEAN DEFAULT TRUE;

-- Add table_type to tables
ALTER TABLE tables
    ADD COLUMN table_type TEXT NOT NULL DEFAULT 'R';
