--
-- Copyright 2025 Tabs Data Inc.
--

CREATE TABLE sessions
(
    access_token_id  TEXT PRIMARY KEY,
    refresh_token_id TEXT UNIQUE NOT NULL,
    user_id          TEXT        NOT NULL,
    role_id          TEXT        NOT NULL,
    created_on       TIMESTAMP   NOT NULL,
    expires_on       TIMESTAMP   NOT NULL,
    status_change_on TIMESTAMP   NOT NULL,
    status           TEXT        NOT NULL
);