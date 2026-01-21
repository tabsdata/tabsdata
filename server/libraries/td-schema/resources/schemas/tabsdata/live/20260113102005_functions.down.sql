--
--  Copyright 2026 Tabs Data Inc.
--

-- Readding with_data subquery.
DROP VIEW table_data_versions__with_function;
CREATE VIEW table_data_versions__with_function AS
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
SELECT tdv.*,
       fr.triggered_on    AS triggered_on,
       fr.triggered_by_id AS triggered_by_id,
       fr.started_on      AS started_on,
       fr.ended_on        AS ended_on,
       fr.status          AS status,
       fv.data_location   AS data_location,
       fv.storage_version AS storage_version,
       wd.with_data_table_data_version_id,
       wd.with_data_column_count,
       wd.with_data_row_count,
       wd.with_data_schema_hash
FROM table_data_versions tdv
         LEFT JOIN functions fv ON tdv.function_version_id = fv.id
         LEFT JOIN function_runs fr ON tdv.function_run_id = fr.id
         LEFT JOIN with_data wd ON tdv.id = wd.tdv_id;

-- Readding tables__read view
CREATE VIEW tables__read AS
SELECT tv.*,
       tv.collection as collection_name,
       tv.function   as function_name,
       tdv.id        as last_data_version
--        tdv.with_data_table_data_version_id as last_data_changed_version
FROM tables__with_names tv
         LEFT JOIN table_data_versions__with_function tdv on tv.id = tdv.table_version_id
WHERE NOT tv.system -- non-system tables only
ORDER BY tdv.triggered_on;

-- Readding dependencies__read
CREATE VIEW dependencies__read AS
SELECT dv.*
FROM dependencies__with_names dv
WHERE NOT dv.system -- non-system tables only
;

-- Readding function_runs views.
-- All function runs in S or RS and all requirements in D or C.
DROP VIEW function_runs__to_execute;
CREATE VIEW function_runs__to_execute AS
SELECT f.*,
       fv.data_location   AS data_location,
       fv.storage_version AS storage_version,
       fv.bundle_id       AS bundle_id
FROM function_runs__with_names f
         LEFT JOIN functions fv ON f.function_version_id = fv.id
WHERE f.status IN ('S', 'RS')
  AND NOT EXISTS
    (SELECT 1
     FROM function_requirements__with_status fr
     WHERE fr.function_run_id = f.id
       AND fr.status NOT IN ('D', 'C'));

-- All function runs in D and all requirements D or C.
DROP VIEW function_runs__to_commit;
CREATE VIEW function_runs__to_commit AS
SELECT f.*
FROM function_runs f
         JOIN
     (SELECT fr.transaction_id
      FROM function_runs fr
               LEFT JOIN function_requirements__with_status req
                         ON fr.transaction_id = req.transaction_id
      GROUP BY fr.transaction_id
      HAVING COUNT(CASE WHEN fr.status IN ('D') THEN 1 END) = COUNT(fr.status)
         AND COUNT(CASE WHEN req.status IN ('D', 'C') THEN 1 END) = COUNT(req.status)) t
     ON f.transaction_id = t.transaction_id;


DROP VIEW function_runs__to_execute;
CREATE VIEW function_runs__to_execute AS
WITH
    runs_with_pending_reqs AS (SELECT DISTINCT function_run_id
                               FROM function_requirements__with_status
                               WHERE status NOT IN ('D', 'C'))
SELECT f.*,
       fv.data_location,
       fv.storage_version,
       fv.bundle_id
FROM function_runs__with_names f
         JOIN functions fv ON f.function_version_id = fv.id
         LEFT JOIN runs_with_pending_reqs p ON f.id = p.function_run_id
WHERE f.status IN ('S', 'RS')
  AND p.function_run_id IS NULL;
