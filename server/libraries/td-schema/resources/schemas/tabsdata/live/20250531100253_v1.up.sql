--
--  Copyright 2025. Tabs Data Inc.
--

-- Add up migration script here

UPDATE tabsdata_system SET value = '1' WHERE name = 'db_version';