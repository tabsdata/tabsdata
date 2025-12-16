--
--  Copyright 2025 Tabs Data Inc.
--

-- Add recursion options column to executions
ALTER TABLE executions
    ADD COLUMN recursion_options TEXT NULL;

-- Add function_id column to executions
ALTER TABLE executions
    ADD COLUMN function_id TEXT NOT NULL DEFAULT '';
UPDATE executions
SET function_id = (SELECT function_id
                   FROM functions
                   WHERE functions.id = executions.function_version_id);
