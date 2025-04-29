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

-- sys_admin user has all permissions by default, but only sa one is fixed
INSERT INTO permissions
SELECT '00000000000000000000000010',
       r.id,
       'sa',
       's',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       true
FROM roles r
WHERE r.name = 'sys_admin'
;

INSERT INTO permissions
SELECT '00000000000000000000000014',
       r.id,
       'ss',
       's',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'sys_admin'
;

INSERT INTO permissions
SELECT '00000000000000000000000100',
       r.id,
       'ca',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'sys_admin'
;

INSERT INTO permissions
SELECT '00000000000000000000000104',
       r.id,
       'cd',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'sys_admin'
;

INSERT INTO permissions
SELECT '00000000000000000000000108',
       r.id,
       'cx',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'sys_admin'
;

INSERT INTO permissions
SELECT '0000000000000000000000010C',
       r.id,
       'cR',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'sys_admin'
;

INSERT INTO permissions
SELECT '0000000000000000000000010G',
       r.id,
       'cr',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'sys_admin'
;

-- other permissions
INSERT INTO permissions
SELECT '0000000000000000000000010K',
       r.id,
       'ss',
       's',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       true
FROM roles r
WHERE r.name = 'sec_admin'
;

INSERT INTO permissions
SELECT '0000000000000000000000010O',
       r.id,
       'ca',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       true
FROM roles r
WHERE r.name = 'sec_admin'
;

INSERT INTO permissions
SELECT '0000000000000000000000010S',
       r.id,
       'cd',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'user'
;

INSERT INTO permissions
SELECT '00000000000000000000000110',
       r.id,
       'cx',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'user'
;

INSERT INTO permissions
SELECT '00000000000000000000000114',
       r.id,
       'cR',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'user'
;

INSERT INTO permissions
SELECT '00000000000000000000000200',
       r.id,
       'cr',
       'c',
       NULL,
       '00000000000000000000000000',
       datetime('now'),
       false
FROM roles r
WHERE r.name = 'user'
;

CREATE VIEW permissions__with_names AS
SELECT p.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || p.granted_by_id || ']') as granted_by,
       r.name                                        as role,
       -- When we support different entity types, we will need to change the
       -- resolution of the entity name to be a CASE statement.
       c.name                                        as entity
FROM permissions p
         LEFT JOIN users u ON p.granted_by_id = u.id
         LEFT JOIN roles r ON p.role_id = r.id
         LEFT JOIN collections c ON p.entity_id = c.id;


CREATE TABLE inter_collection_permissions
(
    id                 TEXT PRIMARY KEY,
    from_collection_id TEXT NOT NULL,
    to_collection_id   TEXT NOT NULL,
    granted_on        TIMESTAMP NOT NULL,
    granted_by_id     TEXT NOT NULL,

    FOREIGN KEY (from_collection_id) REFERENCES collections (id),
    FOREIGN KEY (to_collection_id) REFERENCES collections (id)
);
CREATE UNIQUE INDEX inter_collection_permissions___from_collection_id__to_collection_id__idx
    ON inter_collection_permissions (from_collection_id, to_collection_id);

CREATE VIEW inter_collection_permissions__with_names AS
SELECT p.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || p.granted_by_id || ']') as granted_by,
       c1.name                                       as from_collection,
       c2.name                                       as to_collection
FROM inter_collection_permissions p
         LEFT JOIN users u ON p.granted_by_id = u.id
         LEFT JOIN collections c1 ON p.from_collection_id = c1.id
         LEFT JOIN collections c2 ON p.to_collection_id = c2.id;