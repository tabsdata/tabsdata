--
-- Copyright 2025 Tabs Data Inc.
--

CREATE TABLE permissions
(
    id              TEXT PRIMARY KEY,
    role_id         TEXT      NOT NULL,
    permission_type TEXT      NOT NULL, -- (sa) sys-admin
                                        -- (ss) sec-admin
                                        -- (ca) collection-admin
                                        -- (cd) collection-developer
                                        -- (cx) collection-execution
                                        -- (cr) collection-read
                                        -- (cR) collection-read-all
    entity_type     TEXT      NOT NULL, -- (S)ystem
                                        -- (C)ollection
    entity_id       TEXT      NULL,     -- NULL means ALL
    granted_by_id   TEXT      NOT NULL,
    granted_on      TIMESTAMP NOT NULL,
    fixed           BOOLEAN   NOT NULL,

    FOREIGN KEY (role_id) REFERENCES roles (id)
);
CREATE INDEX permissions___role_id___idx ON permissions (role_id);
CREATE UNIQUE INDEX permissions___role_id__permission_type__entity___idx
    ON permissions (role_id, permission_type, entity_type, entity_id);

CREATE VIEW permissions__with_names AS
SELECT p.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || p.granted_by_id || ']') as granted_by,
       r.name                                        as role,
       -- When we support different entity types, we will need to change the
       -- resolution of the entity name to be a CASE statement.
       c.name                                        as entity_id
FROM permissions p
         LEFT JOIN users u ON p.granted_by_id = u.id
         LEFT JOIN roles r ON p.role_id = r.id
         LEFT JOIN collections c ON p.entity_id = c.id;
