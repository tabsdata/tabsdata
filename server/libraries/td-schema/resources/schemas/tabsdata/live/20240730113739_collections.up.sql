--
-- Copyright 2024 Tabs Data Inc.
--

CREATE TABLE collections
(
    id             TEXT PRIMARY KEY,
    name           TEXT UNIQUE NOT NULL,
    description    TEXT        NOT NULL,
    created_on     TIMESTAMP   NOT NULL,
    created_by_id  TEXT        NOT NULL,
    modified_on    TIMESTAMP   NOT NULL,
    modified_by_id TEXT        NOT NULL
    -- We don't use referential integrity for created_by_id and modified_by_id
    -- because if not we could ever delete a user.
);

CREATE VIEW collections_with_names AS
SELECT p.id,
       p.name,
       p.description,
       p.created_on,
       p.created_by_id,
       -- If the user is deleted, we show the internal id
       IFNULL(u_c.name, '[' || p.created_by_id || ']')  as created_by,
       p.modified_on,
       p.modified_by_id,
       -- If the user is deleted, we show the internal id
       IFNULL(u_m.name, '[' || p.modified_by_id || ']') as modified_by
FROM collections p
         LEFT JOIN users u_c ON p.created_by_id = u_c.id
         LEFT JOIN users u_m ON p.modified_by_id = u_m.id;