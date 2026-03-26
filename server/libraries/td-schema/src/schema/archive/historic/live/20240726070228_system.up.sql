--
-- Copyright 2025 Tabs Data Inc.
--

CREATE TABLE tabsdata_system
(
    name                  TEXT PRIMARY KEY,
    value                 TEXT NULL
);

INSERT INTO tabsdata_system values ('db_version', '0');