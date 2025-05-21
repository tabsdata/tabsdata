--
-- Copyright 2024 Tabs Data Inc.
--

CREATE TABLE collections
(
    id                TEXT PRIMARY KEY,
    name              TEXT UNIQUE NOT NULL,
    description       TEXT        NOT NULL,
    created_on        TIMESTAMP   NOT NULL,
    created_by_id     TEXT        NOT NULL,
    modified_on       TIMESTAMP   NOT NULL,
    modified_by_id    TEXT        NOT NULL,
    -- We don't use referential integrity for created_by_id and modified_by_id
    -- because if not we could ever delete a user.
    name_when_deleted TEXT        NULL
);

-- Collections not deleted
CREATE VIEW collections_active AS
    SELECT id, name, description, created_on, created_by_id, modified_on, modified_by_id
    FROM collections
    WHERE name_when_deleted IS NULL;

CREATE VIEW collections__with_names AS
SELECT p.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u_c.name, '[' || p.created_by_id || ']')  as created_by,
       -- If the user is deleted, we show the internal id
       IFNULL(u_m.name, '[' || p.modified_by_id || ']') as modified_by
FROM collections_active p
         LEFT JOIN users u_c ON p.created_by_id = u_c.id
         LEFT JOIN users u_m ON p.modified_by_id = u_m.id;