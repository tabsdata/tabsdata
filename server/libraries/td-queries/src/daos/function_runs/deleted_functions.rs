//
// Copyright 2026 Tabsdata Inc.
//

//! Runs whose function is gone: deleted, removed with its collection or as a
//! child, so no version of it is current. An update keeps a current version,
//! and its pending runs still run the version they were planned on.

use crate::QueryExecutor;
use crate::daos::function_runs::LogicalRuns;
use crate::primitives::scd2::open_interval_at;
use sea_query::{Asterisk, Expr, ExprTrait, Query, SelectStatement};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::function::FunctionDBIden;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunDBIden};
use td_objects::types::basic::LogicalFunctionRunStatus;

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// The unfinished runs of functions with no current version.
    pub async fn of_deleted_functions(self) -> Result<Vec<LogicalFunctionRunDB>, SqlError> {
        of_deleted_functions_query()
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }
}

pub(crate) const UNFINISHED: &[LogicalFunctionRunStatus] = &[
    LogicalFunctionRunStatus::Scheduled,
    LogicalFunctionRunStatus::Requested,
    LogicalFunctionRunStatus::ReScheduled,
    LogicalFunctionRunStatus::Running,
    LogicalFunctionRunStatus::Done,
    LogicalFunctionRunStatus::Error,
    LogicalFunctionRunStatus::Failed,
    LogicalFunctionRunStatus::OnHold,
];

pub(crate) fn of_deleted_functions_query() -> SelectStatement {
    let current_version = Query::select()
        .expr(Expr::val(1))
        .from(FunctionDBIden::Table)
        .and_where(
            Expr::col((FunctionDBIden::Table, FunctionDBIden::FunctionId)).equals((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::FunctionId,
            )),
        )
        .and_where(open_interval_at((
            FunctionDBIden::Table,
            FunctionDBIden::ValidTo,
        )))
        .take();
    Query::select()
        .expr(Expr::col((LogicalFunctionRunDBIden::Table, Asterisk)))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Status,
            ))
            .is_in(UNFINISHED.iter().map(SeaValue::as_sea_expr)),
        )
        .and_where(Expr::exists(current_version).not())
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;

    #[test]
    fn test_of_deleted_functions_query() {
        let sql = of_deleted_functions_query().to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "logical_function_runs".* FROM "logical_function_runs" WHERE "logical_function_runs"."status" IN ('S', 'RR', 'RS', 'R', 'D', 'E', 'F', 'H') AND (NOT EXISTS(SELECT 1 FROM "functions" WHERE "functions"."function_id" = "logical_function_runs"."function_id" AND "functions"."valid_to" = ('infinity'::timestamptz)))"#
        );
    }
}
