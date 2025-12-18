--
--  Copyright 2025 Tabs Data Inc.
--

-- Drop enabled column to functions
ALTER TABLE functions
    DROP COLUMN enabled;

-- Drop table_type to tables
ALTER TABLE tables
    DROP COLUMN table_type;
