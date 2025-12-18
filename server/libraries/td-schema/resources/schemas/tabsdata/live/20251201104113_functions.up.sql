--
--  Copyright 2025 Tabs Data Inc.
--

-- Add enabled column to functions
ALTER TABLE functions
    ADD COLUMN enabled BOOLEAN DEFAULT TRUE;

