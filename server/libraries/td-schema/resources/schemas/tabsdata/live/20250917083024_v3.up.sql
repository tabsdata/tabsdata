--
--  Copyright 2025 Tabs Data Inc.
--

UPDATE tabsdata_system
SET value = '2'
WHERE name = 'db_version';

ALTER TABLE functions
    ADD COLUMN connector sssasasTEXT NULL;