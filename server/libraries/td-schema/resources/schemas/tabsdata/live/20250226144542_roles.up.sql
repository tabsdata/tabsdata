--
-- Copyright 2025 Tabs Data Inc.
--

CREATE TABLE roles
(
    id             TEXT PRIMARY KEY,
    name           TEXT UNIQUE NOT NULL,
    description    TEXT        NOT NULL,
    created_on     TIMESTAMP   NOT NULL,
    created_by_id  TEXT        NOT NULL,
    modified_on    TIMESTAMP   NOT NULL,
    modified_by_id TEXT        NOT NULL,
    fixed          BOOLEAN     NOT NULL -- Fixed roles cannot be deleted.
);
CREATE UNIQUE INDEX roles___name___idx ON roles (name);

-- Create the default roles generating a random id (that is not a UUID): sys_admin, sec_admin, user.
-- Created and modified by IDs are set to '0000000000000000000000' to indicate that the system did it.
INSERT INTO roles
SELECT substr(abs(random()) || abs(random()) || abs(random()) || abs(random()) || abs(random()), 1, 26),
       'sys_admin',
       'System Administrator Role',
       datetime('now'),
       '0000000000000000000000',
       datetime('now'),
       '0000000000000000000000',
       1
;

INSERT INTO roles
SELECT substr(abs(random()) || abs(random()) || abs(random()) || abs(random()) || abs(random()), 1, 26),
       'sec_admin',
       'Security Administrator Role',
       datetime('now'),
       '0000000000000000000000',
       datetime('now'),
       '0000000000000000000000',
       1
;

INSERT INTO roles
SELECT substr(abs(random()) || abs(random()) || abs(random()) || abs(random()) || abs(random()), 1, 26),
       'user',
       'User Role',
       datetime('now'),
       '0000000000000000000000',
       datetime('now'),
       '0000000000000000000000',
       1
;

CREATE VIEW roles__with_names AS
SELECT r.id,
       r.name,
       r.description,
       r.created_on,
       r.created_by_id,
       IFNULL(u_c.name, '[' || r.created_by_id || ']')  as created_by,
       r.modified_on,
       r.modified_by_id,
       IFNULL(u_m.name, '[' || r.modified_by_id || ']') as modified_by,
       r.fixed
FROM roles r
         LEFT JOIN users u_c ON r.created_by_id = u_c.id
         LEFT JOIN users u_m ON r.modified_by_id = u_m.id
;

CREATE TABLE users_roles
(
    id          TEXT PRIMARY KEY,
    user_id     TEXT      NOT NULL,
    role_id     TEXT      NOT NULL,
    added_on    TIMESTAMP NOT NULL,
    added_by_id TEXT      NOT NULL,
    fixed       BOOLEAN   NOT NULL, -- Fixed users' roles cannot be deleted (except if user is).

    FOREIGN KEY (role_id) REFERENCES roles (id)
);
CREATE UNIQUE INDEX roles___user_id__role_id___idx ON users_roles (user_id, role_id);

-- Assign the sys_admin, sec_admin and user roles to the admin user.
INSERT INTO users_roles
SELECT substr(abs(random()) || abs(random()) || abs(random()) || abs(random()) || abs(random()), 1, 26),
       u.id,
       r.id,
       datetime('now'),
       '0000000000000000000000',
       1
FROM users u,
     roles r
WHERE u.name = 'admin'
  AND r.name = 'sec_admin'
;

INSERT INTO users_roles
SELECT substr(abs(random()) || abs(random()) || abs(random()) || abs(random()) || abs(random()), 1, 26),
       u.id,
       r.id,
       datetime('now'),
       '0000000000000000000000',
       1
FROM users u,
     roles r
WHERE u.name = 'admin'
  AND r.name = 'sys_admin'
;

INSERT INTO users_roles
SELECT substr(abs(random()) || abs(random()) || abs(random()) || abs(random()) || abs(random()), 1, 26),
       u.id,
       r.id,
       datetime('now'),
       '0000000000000000000000',
       1
FROM users u,
     roles r
WHERE u.name = 'admin'
  AND r.name = 'user'
;

CREATE VIEW users_roles__with_names AS
SELECT ur.id,
       ur.user_id,
       IFNULL(u.name, '[' || ur.user_id || ']')       as user,
       ur.role_id,
       IFNULL(r.name, '[' || ur.role_id || ']')       as role,
       ur.added_on,
       ur.added_by_id,
       IFNULL(u_a.name, '[' || ur.added_by_id || ']') as added_by,
       ur.fixed
FROM users_roles ur
         LEFT JOIN users u ON ur.user_id = u.id
         LEFT JOIN roles r ON ur.role_id = r.id
         LEFT JOIN users u_a ON ur.user_id = u_a.id
;