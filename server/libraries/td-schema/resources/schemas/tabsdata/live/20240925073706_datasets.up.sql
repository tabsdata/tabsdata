--
-- Copyright 2024 Tabs Data Inc.
--

CREATE TABLE datasets
(
    id                  TEXT PRIMARY KEY,
    name                TEXT      NOT NULL,
    collection_id        TEXT      NOT NULL,

    created_on          TIMESTAMP NOT NULL,
    created_by_id       TEXT      NOT NULL,
    modified_on         TIMESTAMP NOT NULL,
    modified_by_id      TEXT      NOT NULL,
    -- We don't use referential integrity for created_by_id
    -- because if not we could ever delete a user.

    current_function_id TEXT      NOT NULL, --DN
    current_data_id     TEXT NULL,          --DN
    last_run_on         TIMESTAMP NULL,     --DN
    data_versions       INTEGER   NOT NULL, --DN

    FOREIGN KEY (collection_id) REFERENCES collections (id)
);
CREATE UNIQUE INDEX dataset_name_unique ON datasets (collection_id, name);

CREATE TABLE ds_functions
(
    id                            TEXT PRIMARY KEY,
    name                          TEXT      NOT NULL,
    description                   TEXT      NOT NULL,
    collection_id                 TEXT      NOT NULL, --DN
    dataset_id                    TEXT      NOT NULL,
    data_location                 TEXT      NOT NULL,
    storage_location_version      TEXT      NOT NULL,
    bundle_hash                   TEXT      NOT NULL,
    bundle_avail                  boolean   NOT NULL,
    function_snippet              TEXT NULL,

    execution_template            TEXT NULL,
    execution_template_created_on TIMESTAMP NULL,

    created_on                    TIMESTAMP NOT NULL,
    created_by_id                 TEXT      NOT NULL,
    -- We don't use referential integrity for created_by_id
    -- because if not we could ever delete a user.

    FOREIGN KEY (dataset_id) REFERENCES datasets (id)
);
CREATE INDEX ds_functions_dataset_id ON ds_functions (dataset_id);

CREATE VIEW ds_current_functions AS
SELECT f.id,
       f.name,
       f.description,
       f.collection_id,
       f.dataset_id,
       f.data_location,
       f.storage_location_version,
       f.bundle_hash,
       f.bundle_avail,
       f.function_snippet,
       f.execution_template,
       f.execution_template_created_on,
       f.created_on,
       f.created_by_id
FROM datasets d
         INNER JOIN ds_functions f ON d.current_function_id = f.id;

CREATE TABLE ds_tables
(
    id           TEXT PRIMARY KEY,
    name         TEXT    NOT NULL,
    collection_id TEXT    NOT NULL, --DN
    dataset_id   TEXT    NOT NULL, --DN
    function_id  TEXT    NOT NULL,
    pos          INTEGER NOT NULL, -- position of table as the function output

    FOREIGN KEY (function_id) REFERENCES ds_functions (id)
);
CREATE UNIQUE INDEX ds_tables_name_unique ON ds_tables (function_id, name);

CREATE VIEW ds_system_tables AS
SELECT t.id,
       t.name,
       t.collection_id,
       t.dataset_id,
       t.function_id,
       t.pos
FROM ds_tables t
WHERE t.pos < 0
ORDER BY t.pos;

CREATE VIEW ds_user_tables AS
SELECT t.id,
       t.name,
       t.collection_id,
       t.dataset_id,
       t.function_id,
       t.pos
FROM ds_tables t
WHERE t.pos >= 0
ORDER BY t.pos;

CREATE VIEW ds_current_tables AS
SELECT t.id,
       t.name,
       t.collection_id,
       t.dataset_id,
       t.function_id,
       t.pos
FROM ds_tables t
         INNER JOIN datasets d ON t.function_id = d.current_function_id;

CREATE VIEW ds_current_tables_with_names AS
SELECT t.id,
       t.name,
       t.collection_id,
       ds.name as collection,
       t.dataset_id,
       d.name as dataset,
       t.function_id,
       t.pos
FROM ds_current_tables t
        JOIN collections ds ON t.collection_id = ds.id
        JOIN datasets d ON t.dataset_id = d.id;

CREATE TABLE ds_dependencies
(
    id                 TEXT PRIMARY KEY,
    collection_id       TEXT    NOT NULL, --DN
    dataset_id         TEXT    NOT NULL,
    function_id        TEXT    NOT NULL, --DN

    table_collection_id TEXT    NOT NULL, --DN
    table_dataset_id   TEXT    NOT NULL,
    table_name         TEXT    NOT NULL,
    table_versions     TEXT    NOT NULL,
    pos                INTEGER NOT NULL, -- position of table as the function input

    FOREIGN KEY (dataset_id) REFERENCES datasets (id),
    FOREIGN KEY (function_id) REFERENCES ds_functions (id),
    FOREIGN KEY (table_dataset_id) REFERENCES datasets (id)
);

CREATE VIEW ds_system_dependencies AS
SELECT deps.id,
       deps.collection_id,
       deps.dataset_id,
       deps.function_id,
       deps.table_collection_id,
       deps.table_dataset_id,
       deps.table_name,
       deps.table_versions,
       deps.pos
FROM ds_dependencies deps
WHERE deps.pos < 0
ORDER BY deps.pos;

CREATE VIEW ds_user_dependencies AS
SELECT deps.id,
       deps.collection_id,
       deps.dataset_id,
       deps.function_id,
       deps.table_collection_id,
       deps.table_dataset_id,
       deps.table_name,
       deps.table_versions,
       deps.pos
FROM ds_dependencies deps
WHERE deps.pos >= 0
ORDER BY deps.pos;

CREATE VIEW ds_current_dependencies AS
SELECT deps.id,
       deps.collection_id,
       deps.dataset_id,
       deps.function_id,
       deps.table_collection_id,
       deps.table_dataset_id,
       d.current_function_id as table_function_id,
       deps.table_name,
       deps.table_versions,
       deps.pos
FROM datasets d
     INNER JOIN ds_dependencies deps ON deps.function_id = d.current_function_id;

CREATE TABLE ds_triggers
(
    id                   TEXT PRIMARY KEY,
    collection_id         TEXT NOT NULL, --DN
    dataset_id           TEXT NOT NULL,
    function_id          TEXT NOT NULL, --DN

    trigger_collection_id TEXT NOT NULL, --DN
    trigger_dataset_id   TEXT NOT NULL,

    FOREIGN KEY (dataset_id) REFERENCES datasets (id),
    FOREIGN KEY (function_id) REFERENCES ds_functions (id),
    FOREIGN KEY (trigger_dataset_id) REFERENCES datasets (id)
);

CREATE VIEW ds_current_triggers AS
SELECT tr.id,
       tr.collection_id,
       tr.dataset_id,
       tr.function_id,
       tr.trigger_collection_id,
       tr.trigger_dataset_id,
       d.current_function_id as trigger_function_id
FROM datasets d
         INNER JOIN ds_triggers tr ON tr.trigger_dataset_id = d.id;

CREATE TABLE ds_execution_plans
(
    id              TEXT PRIMARY KEY,
    name            TEXT      NOT NULL,
    collection_id    TEXT      NOT NULL, --DN
    dataset_id      TEXT      NOT NULL, --DN
    function_id     TEXT      NOT NULL,
    plan            TEXT      NOT NULL,

    triggered_by_id TEXT      NOT NULL,
    triggered_on    TIMESTAMP NOT NULL,

    FOREIGN KEY (function_id) REFERENCES ds_functions (id)
);

CREATE VIEW ds_execution_plans_with_state AS
SELECT ep.id,
       ep.name,
       ep.collection_id,
       ep.dataset_id,
       ep.function_id,
       ep.plan,
       ep.triggered_by_id,
       ep.triggered_on,
       MIN(tr.started_on) AS started_on,
       CASE
           WHEN COUNT(*) = SUM(CASE WHEN tr.status IN ('P', 'C') THEN 1 ELSE 0 END)
               THEN MAX(tr.ended_on)
           ELSE NULL
           END            AS ended_on,
       CASE
           WHEN COUNT(*) = SUM(CASE WHEN tr.status = 'P' THEN 1 ELSE 0 END) THEN 'D'
           WHEN COUNT(*) = SUM(CASE WHEN tr.status IN ('P', 'C') THEN 1 ELSE 0 END) THEN 'I'
           WHEN COUNT(*) = SUM(CASE WHEN tr.status = 'S' THEN 1 ELSE 0 END) THEN 'S'
           ELSE 'R'
           END            AS status
FROM ds_execution_plans ep
         LEFT JOIN ds_transactions tr ON ep.id = tr.execution_plan_id
GROUP BY ep.id;

CREATE VIEW ds_execution_plans_with_names AS
SELECT ep.id,
       ep.name,
       ep.collection_id,
       ds.name                                          as collection,
       ep.dataset_id,
       d.name                                           as dataset,
       ep.triggered_by_id,
       -- If the user is deleted, we show the internal id
       IFNULL(u.name, '[' || ep.triggered_by_id || ']') as triggered_by,
       ep.triggered_on,
       ep.started_on,
       ep.ended_on,
       ep.status
FROM ds_execution_plans_with_state ep
         LEFT JOIN collections ds ON ep.collection_id = ds.id
         LEFT JOIN datasets d ON ep.dataset_id = d.id
         LEFT JOIN users u ON ep.triggered_by_id = u.id;

CREATE TABLE ds_transactions
(
    id                TEXT PRIMARY KEY,
    execution_plan_id TEXT      NOT NULL,
    transaction_by    TEXT      NOT NULL,
    transaction_key   TEXT      NOT NULL,
    triggered_by_id   TEXT      NOT NULL,
    triggered_on      TIMESTAMP NOT NULL,
    started_on        TIMESTAMP NULL,
    ended_on          TIMESTAMP NULL,
    commit_id         TEXT      NULL,
    commited_on       TIMESTAMP NULL,
    status            TEXT      NOT NULL,

    FOREIGN KEY (execution_plan_id) REFERENCES ds_execution_plans (id)
);

CREATE VIEW ds_commits AS
SELECT tr.id,
       tr.execution_plan_id,
       tr.transaction_by,
       tr.transaction_key,
       tr.triggered_by_id,
       tr.triggered_on,
       tr.started_on,
       tr.ended_on,
       tr.commit_id,
       tr.commited_on,
       tr.status
FROM ds_transactions tr
WHERE tr.status = 'P'
ORDER BY tr.commit_id DESC;

CREATE TABLE ds_data_versions
(
    id                TEXT PRIMARY KEY,   -- matches ds_execution_requirements.target_data_version
    collection_id      TEXT      NOT NULL, --DN
    dataset_id        TEXT      NOT NULL, --DN
    function_id       TEXT      NOT NULL,
    transaction_id    TEXT      NOT NULL,
    execution_plan_id TEXT      NOT NULL,
    trigger           TEXT      NOT NULL, --DN M (manual), D (dependency)
    triggered_on      TIMESTAMP NOT NULL, --DN
    started_on        TIMESTAMP NULL,     --DN
    ended_on          TIMESTAMP NULL,     --DN
    commit_id         TEXT      NULL,     --DN
    commited_on       TIMESTAMP NULL,     --DN
    status            TEXT      NOT NULL,

    FOREIGN KEY (function_id) REFERENCES ds_functions (id)
);
CREATE INDEX ds_data_versions_dataset_id ON ds_data_versions (dataset_id);

CREATE VIEW ds_data_versions_available AS
SELECT id,
       collection_id,
       dataset_id,
       function_id,
       execution_plan_id,
       trigger,
       triggered_on,
       started_on,
       ended_on,
       commit_id,
       commited_on,
       status
FROM ds_data_versions
WHERE status != 'C'
    ORDER BY triggered_on DESC;

CREATE VIEW ds_data_versions_failed AS
SELECT dv.id,
       dv.collection_id,
       dv.dataset_id,
       dv.function_id,
       dv.transaction_id,
       dv.execution_plan_id,
       dv.trigger,
       dv.triggered_on,
       dv.started_on,
       dv.ended_on,
       dv.commit_id,
       dv.commited_on,
       dv.status
FROM ds_data_versions dv
WHERE dv.status = 'F';

CREATE VIEW ds_data_versions_with_names AS
SELECT dv.id,
       dv.collection_id,
       ds.name as collection_name,
       dv.dataset_id,
       d.name  as dataset_name,
       dv.function_id,
       dv.transaction_id,
       dv.execution_plan_id,
       dv.trigger,
       dv.triggered_on,
       dv.started_on,
       dv.ended_on,
       dv.commit_id,
       dv.commited_on,
       dv.status
FROM ds_data_versions dv
         JOIN collections ds ON dv.collection_id = ds.id
         JOIN datasets d ON dv.dataset_id = d.id;

CREATE TABLE ds_table_data
(
    id                       TEXT PRIMARY KEY,
    collection_id             TEXT NOT NULL, --DN
    dataset_id               TEXT NOT NULL, --DN
    function_id              TEXT NOT NULL, --DN
    data_version_id          TEXT NOT NULL,
    table_id                 TEXT NOT NULL,
    partition                TEXT NULL,
    schema_id                TEXT NOT NULL,
    data_location            TEXT NOT NULL,
    storage_location_version TEXT NOT NULL,

    FOREIGN KEY (data_version_id) REFERENCES ds_data_versions (id),
    FOREIGN KEY (table_id) REFERENCES ds_tables (id)
);

CREATE TABLE ds_execution_requirements
(
    id                               TEXT PRIMARY KEY,
    transaction_id                   TEXT      NOT NULL,
    execution_plan_id                TEXT      NOT NULL,
    execution_plan_triggered_on      TIMESTAMP NOT NULL, --DN

    target_collection_id              TEXT      NOT NULL, --DN
    target_dataset_id                TEXT      NOT NULL, --DN
    target_function_id               TEXT      NOT NULL,
    target_data_version              TEXT      NOT NULL,
    target_existing_dependency_count INTEGER   NOT NULL, -- TOTAL number of required dependencies for the target_data_version

    dependency_collection_id          TEXT NULL,          --DN
    dependency_dataset_id            TEXT NULL,          --DN
    dependency_function_id           TEXT NULL,          --DN
    dependency_table_id              TEXT NULL,
    dependency_pos                   INTEGER NULL,       -- position of the dependency table in the function input
    dependency_data_version          TEXT NULL,
    dependency_formal_data_version   TEXT NULL,
    dependency_data_version_pos      INTEGER NULL,       -- position in the same dependency_data_version

    FOREIGN KEY (transaction_id) REFERENCES ds_transactions (id),
    FOREIGN KEY (execution_plan_id) REFERENCES ds_execution_plans (id),
    FOREIGN KEY (target_function_id) REFERENCES ds_functions (id)
);
CREATE INDEX ds_execution_requirements_ready_to_execute_idx
    ON ds_execution_requirements (target_data_version, target_existing_dependency_count, dependency_data_version);

CREATE VIEW ds_execution_requirements_with_state AS
SELECT er.id,
       er.transaction_id,
       er.execution_plan_id,
       er.execution_plan_triggered_on,

       er.target_collection_id,
       er.target_dataset_id,
       er.target_function_id,
       er.target_data_version,
       er.target_existing_dependency_count,

       er.dependency_collection_id,
       er.dependency_dataset_id,
       er.dependency_function_id,
       er.dependency_table_id,
       er.dependency_pos,
       er.dependency_data_version,
       er.dependency_formal_data_version,
       er.dependency_data_version_pos,

       dv.started_on,
       dv.ended_on,
       dv.status
FROM ds_execution_requirements er
         JOIN ds_data_versions dv ON er.target_data_version = dv.id;

CREATE VIEW ds_datasets_ready_to_execute AS
    -- all rows with the same target_data_version have the same execution_plan_id, target_function_id,
    -- target_dataset_id, target_collection_id, etc.
    -- So we use max() aggregation function to be able to retrieve them because of the group by
SELECT max(er.transaction_id)                                                            as transaction_id,
       max(er.execution_plan_id)                                                         as execution_plan_id,

       max(er.target_collection_id)                                                       as collection_id,
       (SELECT name FROM collections WHERE id = er.target_collection_id)                   as collection_name,
       max(er.target_dataset_id)                                                         as dataset_id,
       (SELECT name FROM datasets WHERE id = er.target_dataset_id)                       as dataset_name,
       max(er.target_function_id)                                                        as function_id,
       er.target_data_version                                                            as data_version,

       (SELECT data_location FROM ds_functions WHERE id = target_function_id)            as data_location,
       (SELECT storage_location_version FROM ds_functions WHERE id = target_function_id) as storage_location_version
FROM ds_execution_requirements_with_state er
         LEFT JOIN ds_data_versions dv ON er.dependency_data_version = dv.id
WHERE COALESCE(dv.status, 'D') IN ('D', 'P')
  AND er.status = 'S'
GROUP BY er.target_data_version
HAVING count(dv.status) = max(er.target_existing_dependency_count)
ORDER BY er.transaction_id;

CREATE VIEW ds_execution_requirement_dependencies AS
SELECT dependency_collection_id                                                               as collection_id,
       (SELECT name FROM collections WHERE id = dependency_collection_id)                      as collection_name,
       dependency_dataset_id                                                                 as dataset_id,
       (SELECT name FROM datasets WHERE id = dependency_dataset_id)                          as dataset_name,
       dependency_function_id                                                                as function_id,
       (SELECT name FROM ds_tables WHERE id = dependency_table_id)                           as table_name,
       dependency_pos                                                                        as pos,

       dependency_data_version                                                               as data_version,
       dependency_formal_data_version                                                        as formal_data_version,
       dependency_data_version_pos                                                           as data_version_pos,

       (SELECT data_location FROM ds_functions WHERE id = dependency_function_id)            as data_location,
       (SELECT storage_location_version
        FROM ds_functions
        WHERE id = dependency_function_id)                                                   as storage_location_version,

       target_data_version
FROM ds_execution_requirements_with_state
WHERE table_name IS NOT NULL
  AND dependency_function_id IS NOT NULL
-- we are sorting by positive pos (0,1,2...), then negative pos (-1,-2,-3,...), then data_version_pos
ORDER BY CASE WHEN pos >= 0 THEN 1 ELSE 2 END,
         ABS(pos),
         data_version_pos;

CREATE VIEW datasets_with_names AS
SELECT d.id,
       d.name,
       f.description,
       d.collection_id,
       ds.name                                          as collection,
       d.created_on,
       d.created_by_id,
       -- If the user is deleted, we show the internal id
       IFNULL(u_c.name, '[' || d.created_by_id || ']')  as created_by,
       d.modified_on,
       d.modified_by_id,
       -- If the user is deleted, we show the internal id
       IFNULL(u_m.name, '[' || d.modified_by_id || ']') as modified_by,
       d.current_function_id,
       d.current_data_id,
       d.last_run_on,
       d.data_versions,
       f.data_location,
       f.bundle_avail,
       f.function_snippet
FROM datasets d
         LEFT JOIN users u_c ON d.created_by_id = u_c.id
         LEFT JOIN users u_m ON d.modified_by_id = u_m.id
         LEFT JOIN collections ds ON d.collection_id = ds.id
         LEFT JOIN ds_functions f ON d.current_function_id = f.id;

CREATE VIEW ds_functions_with_names AS
SELECT f.id,
       f.name,
       f.description,
       f.collection_id,
       ds.name as 'collection', f.dataset_id,
       f.name as 'dataset', f.data_location,
       f.bundle_avail,
       f.function_snippet,
       f.created_on,
       f.created_by_id,
       -- If the user is deleted, we show the internal id
       IFNULL(u_c.name, '[' || f.created_by_id || ']') as 'created_by'
FROM ds_functions f
         LEFT JOIN collections ds ON f.collection_id = ds.id
         LEFT JOIN datasets d ON f.dataset_id = d.id
         LEFT JOIN users u_c ON f.created_by_id = u_c.id
;

CREATE VIEW ds_user_dependencies_with_names AS
SELECT deps.id,
       deps.collection_id,
       deps.dataset_id,
       deps.function_id,
       deps.table_collection_id,
       ds.name as 'table_collection', deps.table_dataset_id,
       d.name as 'table_dataset', deps.table_name,
       deps.table_versions,
       deps.table_collection_id || '/' || deps.table_name || '@' ||
       deps.table_versions as 'uri_with_ids', ds.name || '/' || deps.table_name || '@' || deps.table_versions as 'uri_with_names'
FROM ds_user_dependencies deps
         LEFT JOIN collections ds ON deps.table_collection_id = ds.id
         LEFT JOIN datasets d ON deps.table_dataset_id = d.id
;

CREATE VIEW ds_triggers_with_names AS
SELECT tr.id,
       tr.collection_id,
       tr.dataset_id,
       tr.function_id,
       tr.trigger_collection_id,
       ds.name as 'trigger_collection',
       tr.trigger_collection_id,
       d.name as 'trigger_dataset',
       tr.trigger_dataset_id,
       tr.trigger_collection_id || '/' || tr.trigger_dataset_id as 'uri_with_ids',
       ds.name || '/' || d.name as 'uri_with_names'
FROM ds_triggers tr
         LEFT JOIN collections ds ON tr.trigger_collection_id = ds.id
         LEFT JOIN datasets d ON tr.trigger_dataset_id = d.id
;

CREATE TABLE ds_worker_messages
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT NOT NULL, --DN
    dataset_id          TEXT NOT NULL, --DN
    function_id         TEXT NOT NULL, --DN
    transaction_id      TEXT NOT NULL, --DN
    execution_plan_id   TEXT NOT NULL, --DN
    data_version_id     TEXT NOT NULL,

    FOREIGN KEY (data_version_id) REFERENCES ds_data_versions (id)
);

CREATE VIEW ds_worker_messages_with_names AS
SELECT wm.id,
       cl.name as 'collection',
       wm.collection_id,
       d.name as 'dataset',
       wm.dataset_id,
       f.name as 'function',
       wm.function_id,
       wm.transaction_id,
       p.name as 'execution_plan',
       wm.execution_plan_id,
       dv.id as 'data_version_id',
       dv.started_on as 'started_on',
       dv.status
FROM ds_worker_messages wm
        LEFT JOIN collections cl ON wm.collection_id = cl.id
        LEFT JOIN datasets d ON wm.dataset_id = d.id
        LEFT JOIN ds_functions f ON wm.function_id = f.id
        LEFT JOIN ds_execution_plans p ON wm.execution_plan_id = p.id
        LEFT JOIN ds_data_versions dv ON wm.data_version_id = dv.id
;