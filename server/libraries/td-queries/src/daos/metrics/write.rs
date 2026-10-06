//
// Copyright 2026 Tabsdata Inc.
//

//! The `/metrics` fact writers: append at attempt end, never deduct.

use crate::Queries;
use crate::daos::metrics::load::load_ended_attempts;
use crate::daos::metrics::rows::fact_rows;
use crate::primitives::chunked::{MAX_QUERY_CONDITIONS, warn_if_chunked};
use sea_query::{Alias, Expr, ExprTrait, Func, Query};
use std::collections::{BTreeMap, HashSet};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::access::{InProject, XW};
use td_database::domain::{Apiserver, DomainConnection};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::metrics_facts::{MetricsFactDB, MetricsFactDBIden, MetricsFactInsertDB};
use td_objects::types::basic::{
    AtTime, CurrentAttempt, FactSide, LogicalFunctionRunId, LogicalFunctionRunStatus,
    LogicalTableDataVersionId, PhysicalFunctionRunId, PhysicalFunctionRunStatus,
};

#[derive(sqlx::FromRow)]
struct AttemptRef {
    attempt_id: PhysicalFunctionRunId,
}

#[derive(sqlx::FromRow)]
struct Consumption {
    table_version_id: LogicalTableDataVersionId,
    consumed_on: AtTime,
}

/// A version re-materialized by a later attempt was possibly read before: its
/// new written row inherits the earliest read, like the rung derives it.
async fn inherit_consumption(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    version_ids: &[LogicalTableDataVersionId],
) -> Result<(), SqlError> {
    let mut by_instant: BTreeMap<AtTime, Vec<LogicalTableDataVersionId>> = BTreeMap::new();
    for chunk in version_ids.chunks(MAX_QUERY_CONDITIONS) {
        let mut select = Query::select();
        select
            .column(MetricsFactDBIden::TableVersionId)
            .expr_as(
                Func::min(Expr::col(MetricsFactDBIden::Ts)),
                Alias::new("consumed_on"),
            )
            .from(MetricsFactDBIden::Table)
            .and_where(Expr::col(MetricsFactDBIden::Side).eq(FactSide::Read.as_sea_expr()))
            .and_where(
                Expr::col(MetricsFactDBIden::TableVersionId)
                    .is_in(chunk.iter().map(SeaValue::as_sea_expr)),
            )
            .group_by_col(MetricsFactDBIden::TableVersionId);
        let rows: Vec<Consumption> = select
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(SqlError::on_read::<MetricsFactDB>)?;
        for row in rows {
            by_instant
                .entry(row.consumed_on)
                .or_default()
                .push(row.table_version_id);
        }
    }
    for (consumed_on, ids) in &by_instant {
        versions_consumed(conn, ids, consumed_on).await?;
    }
    Ok(())
}

async fn attempts_with_facts(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    pfr_ids: &[PhysicalFunctionRunId],
) -> Result<HashSet<PhysicalFunctionRunId>, SqlError> {
    let mut found = HashSet::new();
    for chunk in pfr_ids.chunks(MAX_QUERY_CONDITIONS) {
        let mut select = Query::select();
        select
            .column(MetricsFactDBIden::AttemptId)
            .from(MetricsFactDBIden::Table)
            .and_where(Expr::col(MetricsFactDBIden::Side).eq(FactSide::Attempt.as_sea_expr()))
            .and_where(
                Expr::col(MetricsFactDBIden::AttemptId)
                    .is_in(chunk.iter().map(SeaValue::as_sea_expr)),
            );
        let rows: Vec<AttemptRef> = select
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(SqlError::on_read::<MetricsFactDB>)?;
        found.extend(rows.into_iter().map(|r| r.attempt_id));
    }
    Ok(found)
}

/// Call after the attempt's row and its outputs are written; an attempt that
/// already owns facts is skipped.
pub async fn attempts_ended(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    pfr_ids: &[PhysicalFunctionRunId],
) -> Result<(), SqlError> {
    if pfr_ids.is_empty() {
        return Ok(());
    }
    let already = attempts_with_facts(conn, pfr_ids).await?;
    let fresh: Vec<PhysicalFunctionRunId> = pfr_ids
        .iter()
        .filter(|id| !already.contains(id))
        .cloned()
        .collect();
    let attempts = load_ended_attempts(conn, &fresh).await?;
    let mut rows: Vec<MetricsFactInsertDB> = Vec::new();
    for attempt in &attempts {
        rows.extend(fact_rows(attempt).map_err(SqlError::DtoConversionError)?);
    }
    if rows.is_empty() {
        return Ok(());
    }
    conn.chunked().insert(&rows).await?;
    for attempt in &attempts {
        let read: Vec<LogicalTableDataVersionId> = attempt
            .ins
            .iter()
            .map(|v| v.table_version_id.clone())
            .collect();
        versions_consumed(conn, &read, &attempt.ended_on).await?;
    }
    let written: Vec<LogicalTableDataVersionId> = attempts
        .iter()
        .flat_map(|a| a.outs.iter().map(|v| v.table_version_id.clone()))
        .collect();
    inherit_consumption(conn, &written).await?;
    Ok(())
}

/// Keeps the earliest instant, so processing order cannot change the answer.
pub async fn versions_consumed(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    version_ids: &[LogicalTableDataVersionId],
    consumed_on: &AtTime,
) -> Result<(), SqlError> {
    if version_ids.is_empty() {
        return Ok(());
    }
    warn_if_chunked::<MetricsFactDB>(version_ids.len());
    for chunk in version_ids.chunks(MAX_QUERY_CONDITIONS) {
        let mut update = Query::update();
        update
            .table(MetricsFactDBIden::Table)
            .value(
                MetricsFactDBIden::FirstConsumedOn,
                consumed_on.as_sea_expr(),
            )
            .and_where(Expr::col(MetricsFactDBIden::Side).eq(FactSide::Written.as_sea_expr()))
            .and_where(
                Expr::col(MetricsFactDBIden::TableVersionId)
                    .is_in(chunk.iter().map(SeaValue::as_sea_expr)),
            )
            .and_where(
                Expr::col(MetricsFactDBIden::FirstConsumedOn)
                    .is_null()
                    .or(Expr::col(MetricsFactDBIden::FirstConsumedOn)
                        .gt(consumed_on.as_sea_expr())),
            );
        update
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<MetricsFactDB>(e, &[]))?;
    }
    Ok(())
}

/// Only current `Done` attempts follow the run: a failed attempt keeps its
/// outcome, a superseded one the status it had. An in-flight status has no
/// fact to move yet.
pub async fn runs_status_changed(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    lfr_ids: &[LogicalFunctionRunId],
    status: &LogicalFunctionRunStatus,
) -> Result<(), SqlError> {
    if lfr_ids.is_empty() || !status.is_ended() {
        return Ok(());
    }
    warn_if_chunked::<MetricsFactDB>(lfr_ids.len());
    for chunk in lfr_ids.chunks(MAX_QUERY_CONDITIONS) {
        let mut update = Query::update();
        update
            .table(MetricsFactDBIden::Table)
            .value(MetricsFactDBIden::RunStatus, status.as_sea_expr())
            .and_where(
                Expr::col(MetricsFactDBIden::FunctionRunId)
                    .is_in(chunk.iter().map(SeaValue::as_sea_expr)),
            )
            .and_where(
                Expr::col(MetricsFactDBIden::AttemptOutcome)
                    .eq(PhysicalFunctionRunStatus::Done.as_sea_expr()),
            )
            .and_where(
                Expr::col(MetricsFactDBIden::Current).eq(CurrentAttempt::TRUE.as_sea_expr()),
            );
        update
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<MetricsFactDB>(e, &[]))?;
    }
    Ok(())
}

/// Call where a new attempt is minted, by replay or recovery: the earlier
/// attempts' facts stop being current.
pub async fn runs_reattempted(
    conn: &mut DomainConnection<Apiserver, InProject, XW>,
    lfr_ids: &[LogicalFunctionRunId],
) -> Result<(), SqlError> {
    if lfr_ids.is_empty() {
        return Ok(());
    }
    warn_if_chunked::<MetricsFactDB>(lfr_ids.len());
    for chunk in lfr_ids.chunks(MAX_QUERY_CONDITIONS) {
        let mut update = Query::update();
        update
            .table(MetricsFactDBIden::Table)
            .value(
                MetricsFactDBIden::Current,
                CurrentAttempt::FALSE.as_sea_expr(),
            )
            .and_where(
                Expr::col(MetricsFactDBIden::FunctionRunId)
                    .is_in(chunk.iter().map(SeaValue::as_sea_expr)),
            )
            .and_where(
                Expr::col(MetricsFactDBIden::Current).eq(CurrentAttempt::TRUE.as_sea_expr()),
            );
        update
            .build_query()
            .execute(&mut *conn)
            .await
            .map_err(|e| SqlError::on_write::<MetricsFactDB>(e, &[]))?;
    }
    Ok(())
}
