//
// Copyright 2026 Tabsdata Inc.
//

//! v14 → v15 repairs data earlier releases left behind. Two shapes of v3
//! data that v4 carried over: state tables, dependencies and triggers a
//! function delete re-registered past the function's close, which are
//! dropped, and duplicate materializations of one attempt's table, of which
//! only the settled one stays. And two that v13 carried over: transactions
//! all of whose runs ended without an attempt that did, which the v13
//! backfill left with no end, now end at their latest known instant, and
//! their executions with them; and function workers still live after their
//! attempt ended, superseded by a recover, fail at the attempt's end.

use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

const OUTLIVES_ITS_FUNCTION: &str = "NOT EXISTS (
    SELECT 1 FROM functions f
    WHERE f.function_id = r.function_id AND f.valid_from <= r.valid_from AND f.valid_to > r.valid_from
)";

fn drop_orphans() -> Vec<String> {
    vec![
        format!(
            "DELETE FROM tables r WHERE r.status = 'A' AND r.name LIKE 'td.fn\\_state\\_\\_%' \
             AND r.function_id IS NOT NULL AND {OUTLIVES_ITS_FUNCTION}"
        ),
        format!("DELETE FROM dependencies r WHERE {OUTLIVES_ITS_FUNCTION}"),
        format!("DELETE FROM triggers r WHERE {OUTLIVES_ITS_FUNCTION}"),
    ]
}

const RANKED_MATERIALIZATIONS: &str = "\
WITH ranked AS (
    SELECT id, valid_to,
           first_value(id) OVER per_table AS keep_id,
           row_number() OVER per_table AS rank,
           max(valid_to) OVER (PARTITION BY physical_function_run_id, table_id) AS group_valid_to
    FROM physical_table_data_versions
    WINDOW per_table AS (
        PARTITION BY physical_function_run_id, table_id ORDER BY (has_data IS NOT NULL) DESC, id DESC
    )
)";

fn dedupe_materializations() -> Vec<String> {
    vec![
        format!(
            "{RANKED_MATERIALIZATIONS} UPDATE table_partitions t SET table_data_version_id = r.keep_id \
             FROM ranked r WHERE r.rank > 1 AND t.table_data_version_id = r.id"
        ),
        format!(
            "{RANKED_MATERIALIZATIONS} UPDATE physical_table_data_versions t \
             SET with_data_table_data_version_id = r.keep_id \
             FROM ranked r WHERE r.rank > 1 AND t.with_data_table_data_version_id = r.id"
        ),
        format!(
            "{RANKED_MATERIALIZATIONS} UPDATE physical_table_data_versions t SET valid_to = r.group_valid_to \
             FROM ranked r WHERE r.rank = 1 AND t.id = r.id AND t.valid_to <> r.group_valid_to"
        ),
        format!(
            "{RANKED_MATERIALIZATIONS} DELETE FROM physical_table_data_versions t \
             USING ranked r WHERE r.rank > 1 AND t.id = r.id"
        ),
    ]
}

const END_FINISHED_TRANSACTIONS: &str = "\
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
  )";

const END_FINISHED_EXECUTIONS: &str = "\
UPDATE executions e SET ended_on = finished.ended_on
FROM (
    SELECT execution_id, max(ended_on) AS ended_on FROM transactions
    GROUP BY execution_id
    HAVING count(*) = count(ended_on) AND bool_and(status IN ('C', 'X', 'Y'))
) AS finished
WHERE e.id = finished.execution_id AND e.ended_on IS NULL";

const FAIL_WORKERS_OF_ENDED_ATTEMPTS: &str = "\
UPDATE workers w SET status = 'F', ended_on = COALESCE(w.ended_on, p.ended_on, p.valid_from)
FROM physical_function_runs p
WHERE p.id = w.physical_function_run_id
  AND w.worker_type = 'F' AND w.status IN ('RR', 'R')
  AND p.status NOT IN ('RR', 'DP', 'R')";

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 14;
    const TO_VERSION: usize = 15;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        let mut out = drop_orphans();
        out.extend(dedupe_materializations());
        out.extend([
            END_FINISHED_TRANSACTIONS.to_string(),
            END_FINISHED_EXECUTIONS.to_string(),
            FAIL_WORKERS_OF_ENDED_ATTEMPTS.to_string(),
            update_db_version(Self::TO_VERSION),
        ]);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v14_to_v15() {
        validate_delta::<Migration>();
    }
}
