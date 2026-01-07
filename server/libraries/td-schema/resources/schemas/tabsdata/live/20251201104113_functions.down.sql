--
--  Copyright 2025 Tabs Data Inc.
--

-- Re-Add NOT NULL constraints from workers and remove worker_type
CREATE TABLE workers_old
(
    id                  TEXT PRIMARY KEY,
    collection_id       TEXT      NOT NULL,
    execution_id        TEXT      NOT NULL,
    transaction_id      TEXT      NOT NULL,
    function_version_id TEXT      NOT NULL,
    function_run_id     TEXT      NOT NULL,
    message_status      TEXT      NOT NULL, -- Locked/Unlocked
    started_on          TIMESTAMP NOT NULL,
    ended_on            TIMESTAMP NOT NULL,
    status              TEXT      NOT NULL,

    FOREIGN KEY (function_run_id) REFERENCES function_runs (id)
);

INSERT INTO workers_old (id,
                         collection_id,
                         execution_id,
                         transaction_id,
                         function_version_id,
                         function_run_id,
                         message_status,
                         started_on,
                         ended_on,
                         status)
SELECT id,
       collection_id,
       execution_id,
       transaction_id,
       function_version_id,
       function_run_id,
       message_status,
       started_on,
       ended_on,
       status
FROM workers
WHERE execution_id IS NOT NULL
  AND transaction_id IS NOT NULL
  AND function_run_id IS NOT NULL;

DROP VIEW workers__with_names;
DROP TABLE workers;

ALTER TABLE workers_old
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

-- Drop table_type to tables
ALTER TABLE tables
    DROP COLUMN table_type;

-- Drop enabled column to functions
ALTER TABLE functions
    DROP COLUMN enabled;

-- Drop trigger_type column to executions
ALTER TABLE executions
    DROP COLUMN trigger_type;
