--
--  Copyright 2026 Tabs Data Inc.
--

-- Adding data columns to table_data_versions.
ALTER TABLE table_data_versions
    ADD COLUMN with_data_table_data_version_id TEXT NULL;

-- And insert missing information (using same logic we had in the view).
WITH with_data AS (SELECT tdv.id                  AS tdv_id,
                          tdv_latest.id           AS with_data_table_data_version_id,
                          tdv_latest.column_count AS with_data_column_count,
                          tdv_latest.row_count    AS with_data_row_count,
                          tdv_latest.schema_hash  AS with_data_schema_hash
                   FROM table_data_versions tdv
                            LEFT JOIN function_runs fr ON tdv.function_run_id = fr.id
                            LEFT JOIN table_data_versions tdv_latest
                                      ON tdv_latest.id = (SELECT tdv2.id
                                                          FROM table_data_versions tdv2
                                                                   LEFT JOIN function_runs fr2 ON tdv2.function_run_id = fr2.id
                                                          WHERE tdv2.table_id = tdv.table_id
                                                            AND tdv2.has_data = TRUE
                                                            AND fr2.triggered_on <= fr.triggered_on
                                                          ORDER BY fr2.triggered_on DESC
                                                          LIMIT 1))
UPDATE table_data_versions
SET (with_data_table_data_version_id, column_count, row_count, schema_hash) =
        (SELECT with_data.with_data_table_data_version_id,
                with_data.with_data_column_count,
                with_data.with_data_row_count,
                with_data.with_data_schema_hash
         FROM with_data
         WHERE with_data.tdv_id = table_data_versions.id)
WHERE EXISTS(SELECT 1 FROM with_data WHERE with_data.tdv_id = table_data_versions.id);

-- Dropping with_data subquery for performance reasons.
DROP VIEW table_data_versions__with_function;
CREATE VIEW table_data_versions__with_function AS
SELECT tdv.*,
       fr.triggered_on    AS triggered_on,
       fr.triggered_by_id AS triggered_by_id,
       fr.started_on      AS started_on,
       fr.ended_on        AS ended_on,
       fr.status          AS status,
       fv.data_location   AS data_location,
       fv.storage_version AS storage_version
FROM table_data_versions tdv
         LEFT JOIN functions fv ON tdv.function_version_id = fv.id
         LEFT JOIN function_runs fr ON tdv.function_run_id = fr.id;

-- Dropping tables__read and updating usage to tables__with_names.
DROP VIEW tables__read;

-- Dropping dependencies__read and updating usage to dependencies__with_names.
DROP VIEW dependencies__read;

-- Improving function_runs views.
DROP VIEW function_runs__to_execute;
-- All function runs in S or RS and all requirements in D or C.
CREATE VIEW function_runs__to_execute AS
WITH
    -- 1. Find all function runs that have pending (not done or cancelled) requirements.
    runs_with_pending_reqs AS (SELECT DISTINCT function_run_id
                               FROM function_requirements__with_status
                               WHERE status NOT IN ('D', 'C'))
-- 2. Select all function_runs that are scheduled to run and have no pending requirements.
SELECT f.*,
       fv.data_location,
       fv.storage_version,
       fv.bundle_id,
       fv.defined_on
FROM function_runs__with_names f
         JOIN functions fv ON f.function_version_id = fv.id
-- We want to exclude runs that have pending requirements.
         LEFT JOIN runs_with_pending_reqs p ON f.id = p.function_run_id
WHERE f.status IN ('S', 'RS')
  AND p.function_run_id IS NULL;


DROP VIEW function_runs__to_commit;
-- All function runs in D and all requirements D or C.
CREATE VIEW function_runs__to_commit AS
WITH
    -- 1. Find transactions where all associated function_runs have a status of 'D'.
    -- This indicates that all runs for the transaction are done.
    transactions_with_done_runs AS (SELECT transaction_id
                                    FROM function_runs
                                    GROUP BY transaction_id
                                    -- This is an efficient way to check if all statuses for a group are 'D'.
                                    HAVING SUM(status <> 'D') = 0),
    -- 2. Find transactions that have any function_requirements not in a final state ('D' or 'C').
    -- These transactions are not ready to be committed.
    transactions_with_pending_reqs AS (SELECT DISTINCT transaction_id
                                       FROM function_requirements__with_status
                                       WHERE status NOT IN ('D', 'C'))
-- 3. Select all function_runs for transactions that are done and have no pending requirements.
SELECT f.*
FROM function_runs f
-- We only want transactions whose runs are all done.
         JOIN transactions_with_done_runs d ON f.transaction_id = d.transaction_id
-- And we want to exclude transactions that have pending requirements.
         LEFT JOIN transactions_with_pending_reqs p ON f.transaction_id = p.transaction_id
WHERE p.transaction_id IS NULL;