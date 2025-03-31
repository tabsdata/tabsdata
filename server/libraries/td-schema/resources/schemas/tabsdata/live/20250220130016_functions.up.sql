--
-- Copyright 2025 Tabs Data Inc.
--

-- Functions (table & __with_names view)

CREATE TABLE functions
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    name                TEXT      NOT NULL,
    function_version_id TEXT      NOT NULL,
    frozen              BOOLEAN   NOT NULL,

    created_on          TIMESTAMP NOT NULL,
    created_by_id       TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);
CREATE UNIQUE INDEX functions___name__collection_id___idx
    ON functions (name, collection_id);

CREATE VIEW functions__with_names AS
SELECT f.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || f.created_by_id || ']') as created_by,
       c.name                                        as collection
FROM functions f
         LEFT JOIN users u ON f.created_by_id = u.id
         LEFT JOIN collections c ON f.collection_id = c.id;

-- Function Versions  (table & __with_names view)

CREATE TABLE function_versions
(
    id              TEXT PRIMARY KEY,
    collection_id   TEXT      NOT NULL,
    name            TEXT      NOT NULL,
    description     TEXT      NOT NULL,
    runtime_values  TEXT      NOT NULL, -- JSON blob with `envs` & `secrets` info used in decorator
    function_id     TEXT      NOT NULL,
    data_location   TEXT      NOT NULL, -- using '~' when deleted
    storage_version TEXT      NOT NULL, -- using '~' when deleted
    bundle_id       TEXT      NOT NULL, -- using '~' when deleted
    snippet         TEXT      NOT NULL, -- using '~' when deleted
    defined_on      TIMESTAMP NOT NULL,
    defined_by_id   TEXT      NOT NULL,
    status          TEXT      NOT NULL, -- Active/Frozen/Deleted

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);

CREATE VIEW function_versions__with_names AS
SELECT fv.*,
       c.name                                         as collection,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM function_versions fv
         LEFT JOIN collections c ON fv.collection_id = c.id
         LEFT JOIN users u ON fv.defined_by_id = u.id;

-- Tables  (table & __with_names view)

CREATE TABLE tables
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    name                TEXT      NOT NULL,
    function_id         TEXT      NULL,
    function_version_id TEXT      NULL,
    table_version_id    TEXT      NOT NULL,
    frozen              BOOLEAN   NOT NULL,
    private             BOOLEAN   NOT NULL,

    created_on          TIMESTAMP NOT NULL,
    created_by_id       TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);
CREATE UNIQUE INDEX tables___name__collection_id___idx
    ON tables (name, collection_id);

CREATE VIEW tables__with_names AS
SELECT t.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || t.created_by_id || ']') as created_by,
       c.name                                        as collection
FROM tables t
         LEFT JOIN collections c ON t.collection_id = c.id
         LEFT JOIN users u ON t.created_by_id = u.id;

-- Table Versions  (table & __with_names view)

CREATE TABLE table_versions
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    table_id            TEXT      NOT NULL,
    name                TEXT      NOT NULL,
    function_version_id TEXT      NOT NULL, -- using '~' when deleted
    function_param_pos  INTEGER   NULL,
    private             BOOLEAN   NOT NULL,

    defined_on          TIMESTAMP NOT NULL,
    defined_by_id       TEXT      NOT NULL,
    status              TEXT      NOT NULL, -- Active/Frozen/Deleted

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id)
);

CREATE VIEW table_versions__with_names AS
SELECT tv.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || tv.defined_by_id || ']') as defined_by,
       c.name                                         as collection,
       fv.name                                        as function
FROM table_versions tv
         LEFT JOIN collections c ON tv.collection_id = c.id
         LEFT JOIN function_versions fv ON tv.function_version_id = fv.id
         LEFT JOIN users u ON tv.defined_by_id = u.id;

-- Dependencies  (table & __with_names view)

CREATE TABLE dependencies
(
    id                    TEXT PRIMARY KEY,
    collection_id         TEXT NOT NULL,
    function_id           TEXT NOT NULL,
    function_version_id   TEXT NOT NULL,
    dependency_version_id TEXT NOT NULL,
    table_collection_id   TEXT NOT NULL,
    table_id              TEXT NOT NULL,
    table_name            TEXT NOT NULL,
    table_versions        TEXT NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);

CREATE VIEW dependencies__with_names AS
SELECT d.*,
       c.name  as collection,
       tc.name as table_collection
FROM dependencies d
         LEFT JOIN collections c ON d.collection_id = c.id
         LEFT JOIN collections tc ON d.table_collection_id = tc.id;

-- Dependency Versions  (table & __with_names view)

CREATE TABLE dependency_versions
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    dependency_id       TEXT      NOT NULL,
    function_id         TEXT      NOT NULL,
    function_version_id TEXT      NOT NULL,

    table_collection_id TEXT      NOT NULL,
    table_id            TEXT      NOT NULL,
    table_name          TEXT      NOT NULL,
    table_versions      TEXT      NOT NULL,

    dep_pos             INTEGER   NOT NULL,

    status              TEXT      NOT NULL, -- Active/Deleted

    defined_on          TIMESTAMP NOT NULL,
    defined_by_id       TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id)
);

CREATE VIEW dependency_versions__with_names AS
SELECT dv.*,
       c.name                                         as collection,
       fv.name                                        as function,

       tc.name                                        as trigger_by_collection,
       tc.name                                        as table_collection,

       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM dependency_versions dv
         LEFT JOIN collections c ON dv.collection_id = c.id
         LEFT JOIN function_versions fv ON dv.function_version_id = fv.id
         LEFT JOIN users u ON dv.defined_by_id = u.id
         LEFT JOIN collections tc ON dv.table_collection_id = tc.id;

-- Triggers  (table & __with_names view)

CREATE TABLE triggers
(
    id                       TEXT PRIMARY KEY,
    collection_id            TEXT NOT NULL,
    function_id              TEXT NOT NULL,
    trigger_version_id       TEXT NOT NULL,

    trigger_by_collection_id TEXT NOT NULL,
    trigger_by_function_id   TEXT NOT NULL,
    trigger_by_table_id      TEXT NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);

CREATE VIEW triggers__with_names AS
SELECT tr.*,
       c.name  as collection,
       tc.name as trigger_by_collection,
       t.name  as trigger_by_table_name
FROM triggers tr
         LEFT JOIN collections c ON tr.collection_id = c.id
         LEFT JOIN tables t ON tr.trigger_by_table_id = t.id
         LEFT JOIN collections tc ON tr.trigger_by_collection_id = tc.id;

-- Trigger Versions  (table & __with_names view)

CREATE TABLE trigger_versions
(
    id                             TEXT PRIMARY KEY,
    collection_id                  TEXT      NOT NULL,
    trigger_id                     TEXT      NOT NULL,
    function_id                    TEXT      NOT NULL,
    function_version_id            TEXT      NOT NULL,

    trigger_by_collection_id       TEXT      NOT NULL,
    trigger_by_function_id         TEXT      NOT NULL,
    trigger_by_function_version_id TEXT      NOT NULL,
    trigger_by_table_id            TEXT      NOT NULL,

    status                         TEXT      NOT NULL, -- Active/Deleted

    defined_on                     TIMESTAMP NOT NULL,
    defined_by_id                  TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id)
);

CREATE VIEW trigger_versions__with_names AS
SELECT tv.*,
       c.name                                         as collection,
       fv.name                                        as function,

       tc.name                                        as trigger_by_collection,
       tfv.name                                       as trigger_by_function,
       t.name                                         as trigger_by_table_name,

       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM trigger_versions tv
         LEFT JOIN collections c ON tv.collection_id = c.id
         LEFT JOIN function_versions fv ON tv.function_version_id = fv.id
         LEFT JOIN users u ON tv.defined_by_id = u.id
         LEFT JOIN collections tc ON tv.trigger_by_collection_id = tc.id
         LEFT JOIN function_versions tfv ON tv.trigger_by_function_version_id = tfv.id
         LEFT JOIN tables t ON tv.trigger_by_table_id = t.id
;
