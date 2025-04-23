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
    partitioned         BOOLEAN   NULL,

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
    partitioned         BOOLEAN   NULL,

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

-- Bundles

CREATE TABLE bundles
(
    id            TEXT PRIMARY KEY,
    collection_id TEXT      NOT NULL,
    hash          TEXT      NOT NULL,

    created_on    TIMESTAMP NOT NULL,
    created_by_id TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);

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
    id                        TEXT PRIMARY KEY,
    collection_id             TEXT      NOT NULL,
    dependency_id             TEXT      NOT NULL,
    function_id               TEXT      NOT NULL,
    function_version_id       TEXT      NOT NULL,

    table_collection_id       TEXT      NOT NULL,
    table_function_version_id TEXT      NOT NULL,
    table_id                  TEXT      NOT NULL,
    table_version_id          TEXT      NOT NULL,
    table_name                TEXT      NOT NULL,
    table_versions            TEXT      NOT NULL,

    dep_pos                   INTEGER   NOT NULL,

    status                    TEXT      NOT NULL, -- Active/Deleted

    defined_on                TIMESTAMP NOT NULL,
    defined_by_id             TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id)
);

CREATE VIEW dependency_versions__with_names AS
SELECT dv.*,
       c.name                                         as collection,
       fv.name                                        as function,

       tc.name                                        as trigger_by_collection,
       tc.name                                        as table_collection,
       tfv.name                                       as table_function,

       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM dependency_versions dv
         LEFT JOIN collections c ON dv.collection_id = c.id
         LEFT JOIN function_versions fv ON dv.function_version_id = fv.id
         LEFT JOIN users u ON dv.defined_by_id = u.id
         LEFT JOIN collections tc ON dv.table_collection_id = tc.id
         LEFT JOIN function_versions tfv ON dv.table_function_version_id = tfv.id;

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
    trigger_by_table_version_id    TEXT      NOT NULL,

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
         LEFT JOIN tables t ON tv.trigger_by_table_id = t.id;

-- Executions  (table & __with_status view)

CREATE TABLE executions
(
    id                  TEXT PRIMARY KEY,
    name                TEXT      NULL,
    collection_id       TEXT      NOT NULL,
    function_version_id TEXT      NOT NULL,

    triggered_on        TIMESTAMP NOT NULL,
    triggered_by_id     TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id)
);

CREATE VIEW executions__with_status AS
SELECT e.*,
       MIN(t.started_on) AS started_on,
       MAX(t.ended_on)   AS ended_on,
       CASE
           WHEN COUNT(CASE WHEN t.status NOT IN ('S') THEN 1 END) = 0 THEN 'S'
           WHEN COUNT(CASE WHEN t.status NOT IN ('P') THEN 1 END) = 0 THEN 'D'
           WHEN COUNT(CASE WHEN t.status NOT IN ('C', 'P') THEN 1 END) = 0 THEN 'I'
           ELSE 'R'
           END           AS status
FROM executions e
         LEFT JOIN transactions__with_status t ON t.execution_id = e.id
GROUP BY e.id;

-- Transactions  (table & __with_status view)

CREATE TABLE transactions
(
    id              TEXT PRIMARY KEY,

    execution_id    TEXT      NOT NULL,

    transaction_by  TEXT      NOT NULL,
    transaction_key TEXT      NOT NULL,

    triggered_on    TIMESTAMP NOT NULL,
    triggered_by_id TEXT      NOT NULL,

    FOREIGN KEY (execution_id) REFERENCES executions (id)
);

CREATE VIEW transactions__with_status AS
SELECT t.*,
       MIN(fr.started_on) AS started_on,
       MAX(fr.ended_on)   AS ended_on,
       CASE
           WHEN COUNT(CASE WHEN fr.status NOT IN ('S') THEN 1 END) = 0 THEN 'S'
           WHEN COUNT(CASE WHEN fr.status NOT IN ('D') THEN 1 END) = 0 THEN 'P'
           WHEN COUNT(CASE WHEN fr.status = 'F' THEN 1 END) > 0 THEN 'F'
           WHEN COUNT(CASE WHEN fr.status = 'H' THEN 1 END) > 0 THEN 'H'
           WHEN COUNT(CASE WHEN fr.status = 'C' THEN 1 END) > 0 THEN 'C'
           ELSE 'R'
           END            AS status
FROM transactions t
         LEFT JOIN function_runs fr ON fr.transaction_id = t.id
GROUP BY t.id;

-- Function runs  (table)

CREATE TABLE function_runs
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    function_version_id TEXT      NOT NULL,

    execution_id        TEXT      NOT NULL,
    transaction_id      TEXT      NOT NULL,

    triggered_on        TIMESTAMP NOT NULL,
    triggered_by_id     TEXT      NOT NULL,
    trigger             TEXT      NOT NULL, -- M (manual), D (dependency)
    started_on          TIMESTAMP NULL,
    ended_on            TIMESTAMP NULL,
    status              TEXT      NOT NULL, -- Scheduled/RunRequested/ReScheduled/Running/Done/Error/Failed/Hold/Canceled

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (execution_id) REFERENCES executions (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id)
);

CREATE VIEW executable_function_runs AS
SELECT f.*,

       fv.data_location   AS data_location,
       fv.storage_version AS storage_version,
       fv.bundle_id       AS bundle_id,

       fv.name            AS name,
       c.name             AS collection,
       e.name             AS execution
FROM function_runs f
         LEFT JOIN collections c ON f.collection_id = c.id
         LEFT JOIN function_versions fv ON f.function_version_id = fv.id
         LEFT JOIN executions e ON f.execution_id = e.id
WHERE (f.status = 'S' OR f.status = 'RS')
  AND NOT EXISTS (SELECT 1
                  FROM function_requirements__with_names fr
                  WHERE fr.function_run_id = f.id
                    AND fr.status != 'D');

-- Data Versions  (table & __with_status & __with_names view)

CREATE TABLE table_data_versions
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT    NOT NULL,
    table_id            TEXT    NOT NULL,
    table_version_id    TEXT    NOT NULL,
    function_version_id TEXT    NOT NULL,

    has_data            BOOLEAN NULL, -- only true/false when published

    execution_id        TEXT    NOT NULL,
    transaction_id      TEXT    NOT NULL,
    function_run_id     TEXT    NOT NULL,

    function_param_pos  INTEGER NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (execution_id) REFERENCES executions (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id),
    FOREIGN KEY (function_run_id) REFERENCES function_runs (id)
);

CREATE VIEW table_data_versions__with_status AS
SELECT tdv.*,
       fr.triggered_on    as triggered_on,
       fr.triggered_by_id as triggered_by_id,
       fr.status          as status,
       tv.partitioned     as partitioned
FROM table_data_versions tdv
         LEFT JOIN function_runs fr ON tdv.function_run_id = fr.id
         LEFT JOIN table_versions tv ON tdv.table_version_id = tv.id;

CREATE VIEW table_data_versions__active AS
SELECT tdv.*
FROM table_data_versions__with_status tdv
WHERE tdv.status NOT IN ('C');

CREATE VIEW table_data_versions__with_names AS
SELECT tdv.*,
       c.name                                            as collection,
       tv.name                                           as name,
       fv.name                                           as function,

       IFNULL(u.name, '[' || tdv.triggered_by_id || ']') as triggered_by
FROM table_data_versions__with_status tdv
         LEFT JOIN collections c ON tdv.collection_id = c.id
         LEFT JOIN table_versions tv ON tdv.table_version_id = tv.id
         LEFT JOIN function_versions fv ON tdv.function_version_id = fv.id
         LEFT JOIN users u ON tdv.triggered_by_id = u.id;

-- Partitions  (table & __with_names view)

CREATE TABLE table_partitions
(
    id                    TEXT PRIMARY KEY,
    collection_id         TEXT    NOT NULL,
    table_id              TEXT    NOT NULL,
    table_version_id      TEXT    NOT NULL,
    function_version_id   TEXT    NOT NULL,
    table_data_version_id TEXT    NOT NULL,

    partition_key         TEXT    NULL,
    partition_deleted     BOOLEAN NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES function_versions (id),
    FOREIGN KEY (table_data_version_id) REFERENCES table_data_versions (id)
);

-- Execution Requirements  (table & __with_names view)

CREATE TABLE function_requirements
(
    id                                TEXT PRIMARY KEY,
    collection_id                     TEXT    NOT NULL,
    execution_id                      TEXT    NOT NULL,
    transaction_id                    TEXT    NOT NULL,

    function_run_id                   TEXT    NOT NULL,
    requirement_table_id              TEXT    NOT NULL,
    requirement_table_version_id      TEXT    NOT NULL,
    requirement_function_run_id       TEXT    NULL,
    requirement_table_data_version_id TEXT    NULL,
    requirement_dependency_pos        INTEGER NULL,
    requirement_version_pos           INTEGER NOT NULL
);

CREATE VIEW function_requirements__with_names AS
SELECT r.*,
       c.name  as collection,
       fv.name as function,
       tv.name as requirement_table,
       CASE
           WHEN r.requirement_table_data_version_id IS NULL THEN 'D'
           ELSE tdv.status
           END AS status
FROM function_requirements r
         LEFT JOIN collections c ON r.collection_id = c.id
         LEFT JOIN table_versions tv ON r.requirement_table_version_id = tv.id
         LEFT JOIN function_versions fv ON r.requirement_function_run_id = fv.id
         LEFT JOIN table_data_versions__with_status tdv ON r.requirement_table_data_version_id = tdv.id;

-- Worker Messages  (table)

CREATE TABLE worker_messages
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT NOT NULL,
    execution_id        TEXT NOT NULL,
    transaction_id      TEXT NOT NULL,
    function_version_id TEXT NOT NULL,
    function_run_id     TEXT NOT NULL,
    status              TEXT NOT NULL, -- Locked/Unlocked

    FOREIGN KEY (function_run_id) REFERENCES function_runs (id)
);