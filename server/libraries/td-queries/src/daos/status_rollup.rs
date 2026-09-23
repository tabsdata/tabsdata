//
// Copyright 2026 Tabsdata Inc.
//

//! No lock of its own: every logical and physical run writer holds the
//! project's exclusive-write lock, so two recomputes of one parent never
//! interleave.

use crate::primitives::chunked::{MAX_QUERY_CONDITIONS, warn_if_chunked};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::access::InProject;
use td_database::domain::{Apiserver, DomainConnection};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{LogicalFunctionRunId, PhysicalFunctionRunId};
use td_schema::schema::current::{Changed, execution_rollup, transaction_rollup};

pub(crate) async fn logical_runs_changed(
    conn: &mut DomainConnection<Apiserver, InProject>,
    lfr_ids: &[LogicalFunctionRunId],
) -> Result<(), SqlError> {
    recompute(conn, Changed::LogicalRuns, lfr_ids).await
}

pub(crate) async fn physical_runs_changed(
    conn: &mut DomainConnection<Apiserver, InProject>,
    pfr_ids: &[PhysicalFunctionRunId],
) -> Result<(), SqlError> {
    recompute(conn, Changed::PhysicalRuns, pfr_ids).await
}

async fn recompute(
    conn: &mut DomainConnection<Apiserver, InProject>,
    changed: Changed,
    ids: &[impl SeaValue],
) -> Result<(), SqlError> {
    if ids.is_empty() {
        return Ok(());
    }
    warn_if_chunked::<ExecutionDB>(ids.len());
    for chunk in ids.chunks(MAX_QUERY_CONDITIONS) {
        let values = || chunk.iter().map(SeaValue::as_sea_value);
        execution_rollup()
            .recompute_for(changed, values())
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<ExecutionDB>(e, &[]))?;
        transaction_rollup()
            .recompute_for(changed, values())
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<TransactionDB>(e, &[]))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use td_objects::types::basic::{ExecutionStatus, LogicalFunctionRunStatus, TransactionStatus};
    use td_schema::schema::current::{RollupArm, RollupDef};

    fn run_statuses(letters: &[&str]) -> Vec<LogicalFunctionRunStatus> {
        letters
            .iter()
            .map(|letter| {
                LogicalFunctionRunStatus::from_str(letter)
                    .unwrap_or_else(|_| panic!("{letter} is not a run status"))
            })
            .collect()
    }

    fn check_ladder<P: FromStr>(rollup: &RollupDef)
    where
        P::Err: std::fmt::Debug,
    {
        for arm in rollup.ladder {
            let (all, any, status) = match arm {
                RollupArm::All { all, status } => (*all, &[][..], status),
                RollupArm::AllWithAny { all, any, status } => (*all, *any, status),
                RollupArm::Any { any, status } => (&[][..], *any, status),
            };
            run_statuses(all);
            run_statuses(any);
            P::from_str(status).unwrap_or_else(|_| panic!("{status} is not a parent status"));
        }
        P::from_str(rollup.fallback).unwrap();
        assert_eq!(
            run_statuses(rollup.finished),
            [
                LogicalFunctionRunStatus::Committed,
                LogicalFunctionRunStatus::Canceled,
                LogicalFunctionRunStatus::Yanked,
            ]
        );
    }

    #[test]
    fn execution_ladder_names_real_statuses() {
        check_ladder::<ExecutionStatus>(&execution_rollup());
    }

    #[test]
    fn transaction_ladder_names_real_statuses() {
        check_ladder::<TransactionStatus>(&transaction_rollup());
    }
}
