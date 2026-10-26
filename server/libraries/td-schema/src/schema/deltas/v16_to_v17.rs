//
// Copyright 2026 Tabsdata Inc.
//

//! v16 → v17: an execution no longer records how its plan was walked; its
//! runs and their inputs record the plan itself. A cancel before 2.2.0 left
//! its runs' attempts and workers live; they end where a cancel ends them
//! now, and their transactions and executions roll up from the new ends.
//! A live attempt under a Committed or Yanked run is a replay, never such a
//! leftover, and stays live.

use crate::schema::deltas::v12_to_v13::status_rollup_of_attempts_in;
use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

/// The attempts this rung ends, whose transactions and executions it rolls
/// up again. Session-temporary and dropped with the upgrade transaction, so a
/// failed upgrade leaves none behind; a rerun starts from a fresh one.
const ENDED_ATTEMPTS: &str = "pg_temp.v17_ended_attempts";

/// A cancel ends an attempt at the transaction's end, or where the attempt
/// began or was dispatched when that came later, as for one a release before
/// 2.2.0 started after cancelling its run. A replay of a canceled run in
/// flight during the upgrade cannot be told apart from such an attempt and
/// ends with them.
const END_ATTEMPTS_OF_CANCELED_RUNS: &str = "\
WITH repaired AS (
  SELECT p.id,
         COALESCE(GREATEST(t.ended_on, p.started_on, p.dispatched_on), now()) AS ended_on
    FROM physical_function_runs p
    JOIN logical_function_runs l ON l.id = p.logical_function_run_id
    JOIN transactions t ON t.id = p.transaction_id
   WHERE p.status IN ('RR', 'DP', 'R')
     AND l.status = 'X'
), failed_workers AS (
  UPDATE workers w SET status = 'F', ended_on = r.ended_on
    FROM repaired r
   WHERE w.physical_function_run_id = r.id
     AND w.worker_type = 'F' AND w.status IN ('RR', 'R')
), ended AS (
  UPDATE physical_function_runs p SET status = 'F', ended_on = r.ended_on
    FROM repaired r
   WHERE p.id = r.id
  RETURNING p.id
)
INSERT INTO pg_temp.v17_ended_attempts SELECT id FROM ended";

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 16;
    const TO_VERSION: usize = 17;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        let mut out = vec![
            "ALTER TABLE executions DROP COLUMN recursion_options".to_string(),
            format!("DROP TABLE IF EXISTS {ENDED_ATTEMPTS}"),
            format!("CREATE TEMPORARY TABLE {ENDED_ATTEMPTS} (id bigint PRIMARY KEY) ON COMMIT DROP"),
            END_ATTEMPTS_OF_CANCELED_RUNS.to_string(),
        ];
        out.extend(status_rollup_of_attempts_in(ENDED_ATTEMPTS));
        out.push(format!("DROP TABLE IF EXISTS {ENDED_ATTEMPTS}"));
        out.push(update_db_version(Self::TO_VERSION));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v16_to_v17() {
        validate_delta::<Migration>();
    }
}
