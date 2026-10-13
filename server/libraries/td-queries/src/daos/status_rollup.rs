//
// Copyright 2026 Tabsdata Inc.
//

//! No lock of its own: write units are `SERIALIZABLE`, so two recomputes of
//! one parent from different children's changes never both commit.
//! Transactions first: an execution rolls up from its transactions.

use crate::primitives::chunked::{MAX_QUERY_CONDITIONS, warn_if_chunked};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{LogicalFunctionRunId, PhysicalFunctionRunId};
use td_schema::schema::current::{Changed, execution_rollup, transaction_rollup};

pub(crate) async fn logical_runs_changed(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    lfr_ids: &[LogicalFunctionRunId],
) -> Result<(), SqlError> {
    recompute(conn, Changed::LogicalRuns, lfr_ids).await
}

pub(crate) async fn physical_runs_changed(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    pfr_ids: &[PhysicalFunctionRunId],
) -> Result<(), SqlError> {
    recompute(conn, Changed::PhysicalRuns, pfr_ids).await
}

async fn recompute(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    changed: Changed,
    ids: &[impl SeaValue],
) -> Result<(), SqlError> {
    if ids.is_empty() {
        return Ok(());
    }
    warn_if_chunked::<ExecutionDB>(ids.len());
    for chunk in ids.chunks(MAX_QUERY_CONDITIONS) {
        let values = || chunk.iter().map(SeaValue::as_sea_expr);
        transaction_rollup()
            .recompute_for(changed, values())
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<TransactionDB>(e, &[]))?;
        execution_rollup()
            .recompute_for(changed, values())
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<ExecutionDB>(e, &[]))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Alias, IntoIden};
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::test_utils::generated;
    use td_error::TdError;
    use td_objects::types::basic::{ExecutionStatus, LogicalFunctionRunStatus, TransactionStatus};
    use td_schema::schema::current::{
        Children, Instants, Parent, Physical, RollupArm, RollupDef, Runs,
    };

    fn check_ladder<P: FromStr, C: FromStr>(rollup: &RollupDef)
    where
        P::Err: std::fmt::Debug,
        C::Err: std::fmt::Debug,
    {
        let children = |letters: &[&str]| {
            for letter in letters {
                C::from_str(letter).unwrap_or_else(|_| panic!("{letter} is not a child status"));
            }
        };
        for arm in rollup.ladder {
            let (all, any, status) = match arm {
                RollupArm::All { all, status } => (*all, &[][..], status),
                RollupArm::AllWithAny { all, any, status } => (*all, *any, status),
                RollupArm::Any { any, status } => (&[][..], *any, status),
            };
            children(all);
            children(any);
            P::from_str(status).unwrap_or_else(|_| panic!("{status} is not a parent status"));
        }
        P::from_str(rollup.fallback).unwrap();
        children(rollup.finished);
    }

    #[test]
    fn execution_ladder_names_real_statuses() {
        check_ladder::<ExecutionStatus, TransactionStatus>(&execution_rollup());
    }

    #[test]
    fn transaction_ladder_names_real_statuses() {
        check_ladder::<TransactionStatus, LogicalFunctionRunStatus>(&transaction_rollup());
    }

    const RUN_STATUSES: &[&str] = &["S", "RR", "RS", "R", "E", "D", "F", "H", "C", "X", "Y"];
    const RUN_IN_FLIGHT: &[&str] = &["S", "RR", "RS", "R", "E"];
    /// What a transaction can read; `U` is kept in the type but never produced.
    const TRANSACTION_STATUSES: &[&str] = &["S", "R", "L", "C", "X", "Y"];

    fn support_set(alphabet: &[&'static str], mask: u32) -> Vec<&'static str> {
        alphabet
            .iter()
            .enumerate()
            .filter(|(bit, _)| mask & (1 << bit) != 0)
            .map(|(_, status)| *status)
            .collect()
    }

    fn mask(alphabet: &[&str], set: &[&str]) -> u32 {
        set.iter()
            .map(|s| 1u32 << alphabet.iter().position(|a| a == s).unwrap())
            .sum()
    }

    /// The production ladder and its rendering, over temp tables holding one
    /// parent per set of child statuses.
    fn over_temp_tables(rollup: &RollupDef) -> RollupDef {
        let iden = |name: &str| Alias::new(name).into_iden();
        RollupDef {
            parent: Parent {
                table: iden("ladder_parents"),
                id: iden("id"),
                status: iden("status"),
                started_on: iden("started_on"),
                ended_on: iden("ended_on"),
            },
            children: Children {
                table: iden("ladder_children"),
                parent: iden("parent_id"),
                status: iden("status"),
            },
            runs: Runs {
                table: iden("ladder_children"),
                id: iden("id"),
                parent: iden("parent_id"),
            },
            physical: Physical {
                table: iden("ladder_attempts"),
                id: iden("id"),
                parent: iden("parent_id"),
                started_on: iden("started_on"),
                ended_on: iden("ended_on"),
            },
            instants: Instants::Attempts,
            ladder: rollup.ladder,
            fallback: rollup.fallback,
            finished: rollup.finished,
        }
    }

    /// Every non-empty set of `alphabet` rolled up by `rollup`'s ladder, as
    /// the database renders it.
    async fn roll_up_every_set(
        db: &TdPool,
        rollup: &RollupDef,
        alphabet: &[&str],
    ) -> BTreeMap<u32, String> {
        let sets = (1i64 << alphabet.len()) - 1;
        let letters = alphabet
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(",");
        let mut trx = db.begin().await.expect("ladder probe");
        for statement in [
            "CREATE TEMP TABLE ladder_parents (id bigint PRIMARY KEY, status text, \
             started_on timestamptz, ended_on timestamptz) ON COMMIT DROP"
                .to_string(),
            "CREATE TEMP TABLE ladder_children (id bigint, parent_id bigint, status text) \
             ON COMMIT DROP"
                .to_string(),
            "CREATE TEMP TABLE ladder_attempts (id bigint, parent_id bigint, \
             started_on timestamptz, ended_on timestamptz) ON COMMIT DROP"
                .to_string(),
            format!(
                "INSERT INTO ladder_parents (id) SELECT mask FROM generate_series(1, {sets}) AS mask"
            ),
            format!(
                "INSERT INTO ladder_children (id, parent_id, status) \
                 SELECT mask * 16 + bit, mask, (ARRAY[{letters}])[bit + 1] \
                 FROM generate_series(1, {sets}) AS mask, generate_series(0, {}) AS bit \
                 WHERE mask & (1 << bit) <> 0",
                alphabet.len() - 1
            ),
        ] {
            sqlx::query(generated(statement))
                .execute(&mut *trx)
                .await
                .expect("ladder probe");
        }
        over_temp_tables(rollup)
            .backfill()
            .build_query()
            .execute(&mut *trx)
            .await
            .expect("ladder probe");
        let rows: Vec<(i64, String)> =
            sqlx::query_as("SELECT id, status FROM ladder_parents ORDER BY id")
                .fetch_all(&mut *trx)
                .await
                .expect("ladder probe");
        trx.rollback().await.expect("ladder probe");
        rows.into_iter()
            .map(|(mask, status)| (mask as u32, status))
            .collect()
    }

    /// Sets a tick has yet to settle: Done awaiting its transaction's commit,
    /// or Done beside Canceled awaiting the discard.
    fn awaiting_settlement(set: &[&str]) -> bool {
        set.contains(&"D")
            && !set.iter().any(|s| matches!(*s, "F" | "H" | "Y"))
            && set.iter().all(|s| matches!(*s, "D" | "C" | "X"))
    }

    /// The transaction ladder over every set of run statuses, as the
    /// rendered roll-up computes it. No set reads Unexpected; a set with
    /// nothing in flight reads a final status unless a tick has yet to settle
    /// it; and a transaction reads Canceled only when nothing in it can still
    /// publish (every run Canceled, or beside Committed ones, which atomic
    /// commits never leave).
    #[td_test::test(db)]
    async fn test_the_transaction_ladder_is_total_and_idle_sets_are_final(
        db: TdPool,
    ) -> Result<(), TdError> {
        let rolled_up = roll_up_every_set(&db, &transaction_rollup(), RUN_STATUSES).await;
        for (mask, status) in &rolled_up {
            let set = support_set(RUN_STATUSES, *mask);
            assert_ne!(status, "U", "{set:?} reads Unexpected");
            let idle = !set.iter().any(|s| RUN_IN_FLIGHT.contains(s));
            if idle && status == "R" {
                assert!(awaiting_settlement(&set), "idle {set:?} reads Running");
            }
            if status == "X" {
                assert!(
                    set.iter().all(|s| matches!(*s, "X" | "C")),
                    "{set:?} reads Canceled"
                );
            }
        }
        Ok(())
    }

    /// The execution ladder over every set of transaction statuses. An
    /// execution may mix outcomes: it never reads Unexpected, reads Running
    /// only while a transaction is scheduled or running, and Finished once
    /// every transaction has ended.
    #[td_test::test(db)]
    async fn test_the_execution_ladder_is_total_and_idle_sets_are_final(
        db: TdPool,
    ) -> Result<(), TdError> {
        let rolled_up = roll_up_every_set(&db, &execution_rollup(), TRANSACTION_STATUSES).await;
        for (mask, status) in &rolled_up {
            let set = support_set(TRANSACTION_STATUSES, *mask);
            assert_ne!(status, "U", "{set:?} reads Unexpected");
            let moving = set.iter().any(|s| matches!(*s, "S" | "R"));
            assert_eq!(
                status == "R",
                moving && set != ["S"],
                "{set:?} reads {status}"
            );
            let ended = set.iter().all(|s| matches!(*s, "C" | "X" | "Y"));
            assert_eq!(status == "F", ended, "{set:?} reads {status}");
        }
        Ok(())
    }

    /// The mixes canceling a transaction beside a failed one produces.
    /// Transaction {Canceled, On Hold} has something to recover, so it is
    /// Stalled; so is an execution holding a Committed, a Stalled and a
    /// Canceled transaction.
    #[td_test::test(db)]
    async fn test_canceling_beside_a_failure_reads_stalled(db: TdPool) -> Result<(), TdError> {
        let transactions = roll_up_every_set(&db, &transaction_rollup(), RUN_STATUSES).await;
        let executions = roll_up_every_set(&db, &execution_rollup(), TRANSACTION_STATUSES).await;
        assert_eq!(transactions[&mask(RUN_STATUSES, &["X", "H"])], "L");
        assert_eq!(
            executions[&mask(TRANSACTION_STATUSES, &["C", "L", "X"])],
            "L"
        );
        Ok(())
    }
}
