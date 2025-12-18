--
--  Copyright 2025 Tabs Data Inc.
--

-- Add enabled column to functions
ALTER TABLE functions
    ADD COLUMN enabled BOOLEAN DEFAULT TRUE;

-- Add table_type to tables
ALTER TABLE tables
    ADD COLUMN table_type TEXT NOT NULL DEFAULT 'R';

-- Remove NOT NULL constraints from workers and add worker_type
CREATE TABLE workers_new
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    execution_id        TEXT      NULL,
    transaction_id      TEXT      NULL,
    function_version_id TEXT      NOT NULL,
    function_run_id     TEXT      NULL,
    message_status      TEXT      NOT NULL, -- Locked/Unlocked
    started_on          TIMESTAMP NULL,
    ended_on            TIMESTAMP NULL,
    status              TEXT      NOT NULL,

    worker_type         TEXT      NOT NULL,
    worker_config       BLOB      NULL
);

INSERT INTO workers_new (id, collection_id, execution_id, transaction_id, function_version_id, function_run_id,
                         message_status, started_on, ended_on, status, worker_type, worker_config)
SELECT id,
       collection_id,
       execution_id,
       transaction_id,
       function_version_id,
       function_run_id,
       message_status,
       started_on,
       ended_on,
       status,
       'F',
       NULL
FROM workers;

DROP VIEW workers__with_names;
DROP TABLE workers;

ALTER TABLE workers_new
    RENAME TO workers;

CREATE VIEW workers__with_names AS
SELECT w.*,
       c.name  as collection,
       e.name  as execution,
       fv.name as function
FROM workers w
         LEFT JOIN collections c ON w.collection_id = c.id
         LEFT JOIN executions e ON w.execution_id = e.id
         LEFT JOIN functions fv ON w.function_version_id = fv.id;
