--
-- Copyright 2025 Tabs Data Inc.
--

-- Functions (table & __with_names view)

CREATE TABLE functions
(
    id              TEXT PRIMARY KEY,
    collection_id   TEXT      NOT NULL,
    name            TEXT      NOT NULL,
    description     TEXT      NOT NULL,
    decorator       TEXT      NOT NULL,
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

CREATE VIEW functions__with_names AS
SELECT fv.*,
       c.name                                         as collection,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM functions fv
         LEFT JOIN collections c ON fv.collection_id = c.id
         LEFT JOIN users u ON fv.defined_by_id = u.id;

-- Tables  (table & __with_names view)

CREATE TABLE tables
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    table_id            TEXT      NOT NULL,
    name                TEXT      NOT NULL,
    function_id         TEXT      NULL,
    function_version_id TEXT      NOT NULL, -- using '~' when deleted
    function_param_pos  INTEGER   NULL,
    private             BOOLEAN   NOT NULL,
    partitioned         BOOLEAN   NULL,

    defined_on          TIMESTAMP NOT NULL,
    defined_by_id       TEXT      NOT NULL,
    status              TEXT      NOT NULL, -- Active/Frozen/Deleted

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (function_version_id) REFERENCES functions (id)
);

CREATE VIEW tables__with_names AS
SELECT tv.*,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || tv.defined_by_id || ']') as defined_by,
       c.name                                         as collection,
       fv.name                                        as function
FROM tables tv
         LEFT JOIN collections c ON tv.collection_id = c.id
         LEFT JOIN functions fv ON tv.function_version_id = fv.id
         LEFT JOIN users u ON tv.defined_by_id = u.id;

CREATE VIEW tables__read AS
SELECT tv.*,
       tv.collection as collection_name,
       tv.function   as function_name,
       tdv.id        as last_data_version
FROM tables__with_names tv
         LEFT JOIN table_data_versions__with_status tdv on tv.id = tdv.table_version_id
WHERE tv.function_param_pos >= 0 -- non-system tables only
ORDER BY tdv.triggered_on;

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
    FOREIGN KEY (function_version_id) REFERENCES functions (id)
);

CREATE VIEW dependencies__with_names AS
SELECT dv.*,
       c.name                                         as collection,
       fv.name                                        as function,

       tc.name                                        as trigger_by_collection,
       tc.name                                        as table_collection,
       tfv.name                                       as table_function,

       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM dependencies dv
         LEFT JOIN collections c ON dv.collection_id = c.id
         LEFT JOIN functions fv ON dv.function_version_id = fv.id
         LEFT JOIN users u ON dv.defined_by_id = u.id
         LEFT JOIN collections tc ON dv.table_collection_id = tc.id
         LEFT JOIN functions tfv ON dv.table_function_version_id = tfv.id;

-- Triggers  (table & __with_names view)

CREATE TABLE triggers
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
    FOREIGN KEY (function_version_id) REFERENCES functions (id)
);

CREATE VIEW triggers__with_names AS
SELECT tv.*,
       c.name                                         as collection,
       fv.name                                        as function,

       tc.name                                        as trigger_by_collection,
       tfv.name                                       as trigger_by_function,
       t.name                                         as trigger_by_table_name,

       IFNULL(u.name, '[' || fv.defined_by_id || ']') as defined_by
FROM triggers tv
         LEFT JOIN collections c ON tv.collection_id = c.id
         LEFT JOIN functions fv ON tv.function_version_id = fv.id
         LEFT JOIN users u ON tv.defined_by_id = u.id
         LEFT JOIN collections tc ON tv.trigger_by_collection_id = tc.id
         LEFT JOIN functions tfv ON tv.trigger_by_function_version_id = tfv.id
         LEFT JOIN tables t ON tv.trigger_by_table_version_id = t.id;

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
    FOREIGN KEY (function_version_id) REFERENCES functions (id)
);

CREATE VIEW executions__with_status AS
SELECT s.*,
       MIN(t.started_on)                                         AS started_on,
       MAX(CASE WHEN s.status IN ('D', 'I') THEN t.ended_on END) AS ended_on
FROM (SELECT e.*,
             CASE
                 WHEN COUNT(CASE WHEN t.status NOT IN ('S', 'RR') THEN 1 END) = 0 THEN 'S'
                 WHEN COUNT(CASE WHEN t.status NOT IN ('P') THEN 1 END) = 0 THEN 'D'
                 WHEN COUNT(CASE WHEN t.status NOT IN ('C', 'P') THEN 1 END) = 0 THEN 'I'
                 ELSE 'R'
                 END AS status
      FROM executions e
               LEFT JOIN transactions__with_status t ON t.execution_id = e.id
      GROUP BY e.id) s
         JOIN executions e ON e.id = s.id
         LEFT JOIN transactions__with_status t ON t.execution_id = e.id
GROUP BY s.id;

CREATE VIEW executions__with_names AS
SELECT e.*,
       c.name                                          AS collection,
       f.name                                          AS function,
       IFNULL(u.name, '[' || e.triggered_by_id || ']') as triggered_by
FROM executions__with_status e
         LEFT JOIN collections c ON e.collection_id = c.id
         LEFT JOIN functions f ON e.function_version_id = f.id
         LEFT JOIN users u ON e.triggered_by_id = u.id;

-- Transactions  (table & __with_status view & __with_names view)

CREATE TABLE transactions
(
    id              TEXT PRIMARY KEY,
    collection_id   TEXT      NOT NULL,
    execution_id    TEXT      NOT NULL,

    transaction_by  TEXT      NOT NULL,
    transaction_key TEXT      NOT NULL,

    triggered_on    TIMESTAMP NOT NULL,
    triggered_by_id TEXT      NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (execution_id) REFERENCES executions (id)
);

CREATE VIEW transactions__with_status AS
SELECT s.*,
       MIN(fr.started_on)                                         AS started_on,
       CASE WHEN s.status IN ('C', 'P') THEN MAX(fr.ended_on) END AS ended_on
FROM (SELECT t.*,
             CASE
                 WHEN COUNT(CASE WHEN fr.status NOT IN ('S', 'RR') THEN 1 END) = 0 THEN 'S'
                 WHEN COUNT(CASE WHEN fr.status NOT IN ('D') THEN 1 END) = 0 THEN 'P'
                 WHEN COUNT(CASE WHEN fr.status = 'F' THEN 1 END) > 0 THEN 'F'
                 WHEN COUNT(CASE WHEN fr.status = 'H' THEN 1 END) > 0 THEN 'H'
                 WHEN COUNT(CASE WHEN fr.status = 'C' THEN 1 END) > 0 THEN 'C'
                 ELSE 'R'
                 END AS status
      FROM transactions t
               LEFT JOIN function_runs fr ON fr.transaction_id = t.id
      GROUP BY t.id) s
         JOIN transactions t ON t.id = s.id
         LEFT JOIN function_runs fr ON fr.transaction_id = t.id
GROUP BY s.id;

CREATE VIEW transactions__with_names AS
SELECT t.*,
       c.name                                          AS collection,
       e.name                                          AS execution,
       IFNULL(u.name, '[' || t.triggered_by_id || ']') as triggered_by
FROM transactions__with_status t
         LEFT JOIN collections c ON t.collection_id = c.id
         LEFT JOIN executions e ON t.execution_id = e.id
         LEFT JOIN users u ON t.triggered_by_id = u.id;

-- Function runs (table & __with_names & executable_ views)

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
    FOREIGN KEY (function_version_id) REFERENCES functions (id)
);

CREATE VIEW function_runs__with_names AS
SELECT f.*,
       fv.data_location                                AS data_location,
       fv.name                                         AS name,
       c.name                                          AS collection,
       e.name                                          AS execution,
       IFNULL(u.name, '[' || f.triggered_by_id || ']') as triggered_by
FROM function_runs f
         LEFT JOIN collections c ON f.collection_id = c.id
         LEFT JOIN functions fv ON f.function_version_id = fv.id
         LEFT JOIN executions e ON f.execution_id = e.id
         LEFT JOIN users u ON f.triggered_by_id = u.id;

CREATE VIEW executable_function_runs AS
SELECT f.*,
       fv.storage_version AS storage_version,
       fv.bundle_id       AS bundle_id
FROM function_runs__with_names f
         LEFT JOIN collections c ON f.collection_id = c.id
         LEFT JOIN functions fv ON f.function_version_id = fv.id
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
    name                TEXT    NOT NULL,
    table_version_id    TEXT    NOT NULL,
    function_version_id TEXT    NOT NULL,

    has_data            BOOLEAN NULL, -- only true/false when finished

    execution_id        TEXT    NOT NULL,
    transaction_id      TEXT    NOT NULL,
    function_run_id     TEXT    NOT NULL,

    function_param_pos  INTEGER NOT NULL,

    FOREIGN KEY (collection_id) REFERENCES collections (id),
    FOREIGN KEY (execution_id) REFERENCES executions (id),
    FOREIGN KEY (function_version_id) REFERENCES functions (id),
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
         LEFT JOIN tables tv ON tdv.table_version_id = tv.id;

CREATE VIEW table_data_versions__active AS
SELECT tdv.*
FROM table_data_versions__with_status tdv
WHERE tdv.status NOT IN ('C');

CREATE VIEW table_data_versions__with_names AS
SELECT tdv.*,
       c.name                                            as collection,
       fv.name                                           as function,

       IFNULL(u.name, '[' || tdv.triggered_by_id || ']') as triggered_by
FROM table_data_versions__with_status tdv
         LEFT JOIN collections c ON tdv.collection_id = c.id
         LEFT JOIN functions fv ON tdv.function_version_id = fv.id
         LEFT JOIN users u ON tdv.triggered_by_id = u.id;

CREATE VIEW table_data_versions__read AS
SELECT tdv.*,
       tdv.collection     as collection_name,
       tdv.name           as table_name,
       tdv.function       as function_name,
       tdv.has_data       as data_changed,
       tdv.triggered_on   as created_at,
       t.status           as transaction_status,
       fv.data_location   as data_location,
       fv.storage_version as storage_version,
       (SELECT tdv2.id
        FROM table_data_versions__with_names tdv2
        WHERE tdv2.table_id = tdv.table_id
          AND tdv2.has_data = TRUE
          AND tdv2.triggered_on <= tdv.triggered_on
        ORDER BY tdv2.triggered_on DESC
        LIMIT 1)          as with_data_table_data_version_id
FROM table_data_versions__with_names tdv
         LEFT JOIN transactions__with_status t ON tdv.transaction_id = t.id
         LEFT JOIN functions fv ON tdv.function_version_id = fv.id;

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
    FOREIGN KEY (function_version_id) REFERENCES functions (id),
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
    requirement_function_version_id   TEXT    NOT NULL,
    requirement_table_version_id      TEXT    NOT NULL,
    requirement_function_run_id       TEXT    NULL,
    requirement_table_data_version_id TEXT    NULL,
    requirement_input_idx             INTEGER NULL,
    requirement_dependency_pos        INTEGER NULL,
    requirement_version_pos           INTEGER NOT NULL
);

CREATE VIEW function_requirements__with_status AS
SELECT r.*,
       CASE
           WHEN r.requirement_table_data_version_id IS NULL THEN 'D'
           ELSE tdv.status
           END AS status
FROM function_requirements r
         LEFT JOIN table_data_versions__with_status tdv ON r.requirement_table_data_version_id = tdv.id;

CREATE VIEW function_requirements__with_names AS
SELECT r.*,
       c.name  as collection,
       fv.name as function,
       tv.name as requirement_table
FROM function_requirements__with_status r
         LEFT JOIN collections c ON r.collection_id = c.id
         LEFT JOIN tables tv ON r.requirement_table_version_id = tv.id
         LEFT JOIN functions fv ON r.requirement_function_version_id = fv.id;

-- Worker Messages  (table & __with_names view)

CREATE TABLE worker_messages
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT NOT NULL,
    execution_id        TEXT NOT NULL,
    transaction_id      TEXT NOT NULL,
    function_version_id TEXT NOT NULL,
    function_run_id     TEXT NOT NULL,
    message_status      TEXT NOT NULL, -- Locked/Unlocked

    FOREIGN KEY (function_run_id) REFERENCES function_runs (id)
);

CREATE VIEW worker_messages__with_names AS
SELECT w.*,
       fr.status as status,
       c.name    as collection,
       e.name    as execution,
       fv.name   as function
FROM worker_messages w
         LEFT JOIN collections c ON w.collection_id = c.id
         LEFT JOIN executions e ON w.execution_id = e.id
         LEFT JOIN functions fv ON w.function_version_id = fv.id
         LEFT JOIN function_runs fr ON w.function_run_id = fr.id;