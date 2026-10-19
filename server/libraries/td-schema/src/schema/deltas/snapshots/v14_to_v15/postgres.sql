-- AUTO-GENERATED schema DDL snapshot. Do not edit by hand.
-- Refresh by rerunning the snapshot test with TD_SNAPSHOT_UPDATE=1.

DELETE FROM tables r WHERE r.status = 'A' AND r.name LIKE 'td.fn\_state\_\_%' AND r.function_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM functions f
    WHERE f.function_id = r.function_id AND f.valid_from <= r.valid_from AND f.valid_to > r.valid_from
)

DELETE FROM dependencies r WHERE NOT EXISTS (
    SELECT 1 FROM functions f
    WHERE f.function_id = r.function_id AND f.valid_from <= r.valid_from AND f.valid_to > r.valid_from
)

DELETE FROM triggers r WHERE NOT EXISTS (
    SELECT 1 FROM functions f
    WHERE f.function_id = r.function_id AND f.valid_from <= r.valid_from AND f.valid_to > r.valid_from
)

WITH ranked AS (
    SELECT id, valid_to,
           first_value(id) OVER per_table AS keep_id,
           row_number() OVER per_table AS rank,
           max(valid_to) OVER (PARTITION BY physical_function_run_id, table_id) AS group_valid_to
    FROM physical_table_data_versions
    WINDOW per_table AS (
        PARTITION BY physical_function_run_id, table_id ORDER BY (has_data IS NOT NULL) DESC, id DESC
    )
) UPDATE table_partitions t SET table_data_version_id = r.keep_id FROM ranked r WHERE r.rank > 1 AND t.table_data_version_id = r.id

WITH ranked AS (
    SELECT id, valid_to,
           first_value(id) OVER per_table AS keep_id,
           row_number() OVER per_table AS rank,
           max(valid_to) OVER (PARTITION BY physical_function_run_id, table_id) AS group_valid_to
    FROM physical_table_data_versions
    WINDOW per_table AS (
        PARTITION BY physical_function_run_id, table_id ORDER BY (has_data IS NOT NULL) DESC, id DESC
    )
) UPDATE physical_table_data_versions t SET with_data_table_data_version_id = r.keep_id FROM ranked r WHERE r.rank > 1 AND t.with_data_table_data_version_id = r.id

WITH ranked AS (
    SELECT id, valid_to,
           first_value(id) OVER per_table AS keep_id,
           row_number() OVER per_table AS rank,
           max(valid_to) OVER (PARTITION BY physical_function_run_id, table_id) AS group_valid_to
    FROM physical_table_data_versions
    WINDOW per_table AS (
        PARTITION BY physical_function_run_id, table_id ORDER BY (has_data IS NOT NULL) DESC, id DESC
    )
) UPDATE physical_table_data_versions t SET valid_to = r.group_valid_to FROM ranked r WHERE r.rank = 1 AND t.id = r.id AND t.valid_to <> r.group_valid_to

WITH ranked AS (
    SELECT id, valid_to,
           first_value(id) OVER per_table AS keep_id,
           row_number() OVER per_table AS rank,
           max(valid_to) OVER (PARTITION BY physical_function_run_id, table_id) AS group_valid_to
    FROM physical_table_data_versions
    WINDOW per_table AS (
        PARTITION BY physical_function_run_id, table_id ORDER BY (has_data IS NOT NULL) DESC, id DESC
    )
) DELETE FROM physical_table_data_versions t USING ranked r WHERE r.rank > 1 AND t.id = r.id

UPDATE transactions t SET ended_on = GREATEST(
    t.triggered_on,
    t.started_on,
    (SELECT max(p.ended_on) FROM physical_function_runs p WHERE p.transaction_id = t.id)
)
WHERE t.ended_on IS NULL
  AND EXISTS (SELECT 1 FROM logical_function_runs r WHERE r.transaction_id = t.id)
  AND NOT EXISTS (
      SELECT 1 FROM logical_function_runs r
      WHERE r.transaction_id = t.id AND r.status NOT IN ('C', 'X', 'Y')
  )

UPDATE executions e SET ended_on = finished.ended_on
FROM (
    SELECT execution_id, max(ended_on) AS ended_on FROM transactions
    GROUP BY execution_id
    HAVING count(*) = count(ended_on) AND bool_and(status IN ('C', 'X', 'Y'))
) AS finished
WHERE e.id = finished.execution_id AND e.ended_on IS NULL

UPDATE workers w SET status = 'F', ended_on = COALESCE(w.ended_on, p.ended_on, p.valid_from)
FROM physical_function_runs p
WHERE p.id = w.physical_function_run_id
  AND w.worker_type = 'F' AND w.status IN ('RR', 'R')
  AND p.status NOT IN ('RR', 'DP', 'R')

UPDATE tabsdata_system SET value = '15' WHERE name = 'db_version'
