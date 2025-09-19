--
-- Copyright 2024 Tabs Data Inc.
--

CREATE TABLE users
(
    id                   TEXT PRIMARY KEY,
    name                 TEXT UNIQUE NOT NULL,
    full_name            TEXT        NOT NULL,
    email                TEXT UNIQUE NULL,
    created_on           TIMESTAMP   NOT NULL,
    created_by_id        TEXT        NOT NULL,
    modified_on          TIMESTAMP   NOT NULL,
    modified_by_id       TEXT        NOT NULL,
    password_hash        TEXT        NOT NULL,
    password_set_on      TIMESTAMP   NOT NULL,
    password_must_change BOOLEAN     NOT NULL,
    enabled              BOOLEAN     NOT NULL
    -- We don't use referential integrity for created_by_id and modified_by_id
    -- because if not we could ever delete a user.
);

INSERT INTO users
SELECT '00000000000000000000000004',
       'admin',
       'Administrator',
       '-',
       datetime('now'),
       '00000000000000000000000000',
       datetime('now'),
       '00000000000000000000000000',
       '$argon2id$v=19$m=19456,t=2,p=1$ULTV//eju00aMbzZXAORyg$4+7GgsoT3e7lHElgWA2v0OYQHr5SBQzYY3pcUy0/YDM',
       datetime('now'),
       0,
       1
;

CREATE VIEW users__with_names AS
SELECT u.*,
       IFNULL(u_c.name, '[' || u.created_by_id || ']')  as created_by,
       IFNULL(u_m.name, '[' || u.modified_by_id || ']') as modified_by
FROM users u
         LEFT JOIN users u_c ON u.created_by_id = u_c.id
         LEFT JOIN users u_m ON u.modified_by_id = u_m.id
