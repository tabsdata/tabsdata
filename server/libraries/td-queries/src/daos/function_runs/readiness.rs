//
// Copyright 2026 Tabsdata Inc.
//

//! Scheduler-side LFR queries: readiness, commit, blocked inputs, the
//! downstream cascade hop and the previous-run lookup.

use crate::QueryExecutor;
use crate::daos::function_runs::LogicalRuns;
use crate::daos::function_runs::chain::{
    Visibility, build_chain, producer_chain, producer_status_in_strict, producer_status_not_in,
    unmaterialized,
};
use crate::primitives::table::claim;
use sea_query::{
    Asterisk, CommonTableExpression, Expr, ExprTrait, Func, Iden, JoinType, Order, Query,
    SelectStatement, WithClause, WithQuery,
};
use ta_objects::Dao;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden,
    LogicalTableDataVersionDBIden,
};
use td_objects::types::basic::{
    AtTime, ExecutionId, FunctionId, LogicalFunctionRunId, LogicalFunctionRunStatus,
};

impl<E> LogicalRuns<E>
where
    E: QueryExecutor,
{
    /// Scheduled / ReScheduled LFRs whose inputs all have a materialization
    /// visible at `at` from a Done or Committed producer, oldest-triggered
    /// first. A result of exactly `limit` rows means there may be more.
    pub async fn to_execute(
        self,
        at: &AtTime,
        limit: u64,
    ) -> Result<Vec<LogicalFunctionRunDB>, SqlError> {
        fetch_lfrs_to_execute_query(at, limit)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    /// LFR ids, within `execution_ids`, whose transaction has every LFR
    /// `Done` and no blocking input on the chain. The caller marks them
    /// `Committed`.
    pub async fn to_commit(
        self,
        execution_ids: &[ExecutionId],
    ) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        if execution_ids.is_empty() {
            return Ok(vec![]);
        }
        fetch_ids_to_commit_query(execution_ids)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    /// The most recent LFR of `function_id` triggered strictly before
    /// `before_time`; `<` excludes the in-flight LFR, whose `triggered_on`
    /// is the caller's argument.
    pub async fn previous_for_function_at(
        self,
        function_id: &FunctionId,
        before_time: &AtTime,
    ) -> Result<Option<LogicalFunctionRunDB>, SqlError> {
        previous_lfr_query(function_id, before_time)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    /// LFR ids in `execution_id` with an upstream LFR that is Failed or
    /// OnHold.
    pub async fn blocked_ids(
        self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        blocked_ids_query(execution_id)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    /// One BFS hop downstream; see [`downstream_one_hop_query`].
    pub async fn downstream_one_hop(
        self,
        producer_lfrs: &[LogicalFunctionRunId],
        exclude_consumers: &[LogicalFunctionRunId],
    ) -> Result<Vec<LogicalFunctionRunId>, SqlError> {
        downstream_one_hop_query(producer_lfrs, exclude_consumers)
            .build_query_scalar()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }
}

/// LFRs consuming an output version of any LFR in `producer_lfrs`, minus
/// `exclude_consumers` and self-loops. The failure cascade walks this hop
/// until the visited set stops growing, threading the visited ids through
/// `exclude_consumers`.
pub(crate) fn downstream_one_hop_query(
    producer_lfrs: &[LogicalFunctionRunId],
    exclude_consumers: &[LogicalFunctionRunId],
) -> SelectStatement {
    let consumer = (
        LogicalFunctionRunInputDBIden::Table,
        LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
    );
    let producer = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
    );
    let mut stmt = Query::select();
    stmt.distinct()
        .column(consumer)
        .from(LogicalFunctionRunInputDBIden::Table)
        .join(
            JoinType::InnerJoin,
            LogicalTableDataVersionDBIden::Table,
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            ))
            .equals((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            )),
        )
        .and_where(Expr::col(producer).is_in(producer_lfrs.iter().map(SeaValue::as_sea_expr)))
        .and_where(Expr::col(consumer).ne(Expr::col(producer)));
    if !exclude_consumers.is_empty() {
        stmt.and_where(
            Expr::col(consumer).is_not_in(exclude_consumers.iter().map(SeaValue::as_sea_expr)),
        );
    }
    stmt.take()
}

fn correlated_chain(
    mut chain: SelectStatement,
    blocking: sea_query::SimpleExpr,
) -> SelectStatement {
    chain
        .column((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::Id,
        ))
        .and_where(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .equals((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Id,
            )),
        )
        .and_where(blocking);
    chain
}

/// Only an upstream that is already Failed or OnHold blocks: an input with no
/// materialization yet is the normal plan-build state of every in-plan
/// dependency, and blocking on it would start the whole downstream OnHold.
pub(crate) fn blocked_ids_query(execution_id: &ExecutionId) -> SelectStatement {
    let chain = correlated_chain(
        producer_chain(),
        producer_status_in_strict(&[
            LogicalFunctionRunStatus::Failed,
            LogicalFunctionRunStatus::OnHold,
        ]),
    );
    Query::select()
        .distinct()
        .column(LogicalFunctionRunDBIden::Id)
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ExecutionId,
            ))
            .eq(execution_id.as_sea_expr()),
        )
        .and_where(Expr::exists(chain))
        .take()
}

pub(crate) fn previous_lfr_query(
    function_id: &FunctionId,
    before_time: &AtTime,
) -> SelectStatement {
    Query::select()
        .columns(LogicalFunctionRunDB::columns())
        .from(LogicalFunctionRunDB::TABLE)
        .and_where(Expr::col(LogicalFunctionRunDBIden::FunctionId).eq(function_id.as_sea_expr()))
        .and_where(Expr::col(LogicalFunctionRunDBIden::TriggeredOn).lt(before_time.as_sea_expr()))
        .order_by(LogicalFunctionRunDBIden::TriggeredOn, Order::Desc)
        .order_by(LogicalFunctionRunDBIden::Id, Order::Desc)
        .limit(1)
        .take()
}

#[derive(Iden, Clone, Copy)]
struct TransactionsWithDoneRuns;

#[derive(Iden, Clone, Copy)]
struct TransactionsWithPendingReqs;

fn not_done_or_committed() -> sea_query::SimpleExpr {
    producer_status_not_in(&[
        LogicalFunctionRunStatus::Done,
        LogicalFunctionRunStatus::Committed,
    ])
}

/// Commit sees whatever is materialized right now, so the chain is unbounded
/// in time, unlike readiness.
pub(crate) fn fetch_ids_to_commit_query(execution_ids: &[ExecutionId]) -> WithQuery {
    let exec_values: Vec<_> = execution_ids.iter().map(SeaValue::as_sea_expr).collect();

    let done_runs = Query::select()
        .column(LogicalFunctionRunDBIden::TransactionId)
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(Expr::col(LogicalFunctionRunDBIden::ExecutionId).is_in(exec_values.clone()))
        .group_by_col(LogicalFunctionRunDBIden::TransactionId)
        .and_having(
            Expr::expr(Func::sum(
                Expr::case(
                    Expr::col(LogicalFunctionRunDBIden::Status)
                        .ne(LogicalFunctionRunStatus::Done.as_sea_expr()),
                    1,
                )
                .finally(0),
            ))
            .eq(0),
        )
        .take();

    let mut pending_reqs = build_chain(Visibility::Open);
    pending_reqs
        .distinct()
        .column((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::TransactionId,
        ))
        .join(
            JoinType::InnerJoin,
            LogicalFunctionRunDBIden::Table,
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Id,
            ))
            .equals((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            )),
        )
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ExecutionId,
            ))
            .is_in(exec_values),
        )
        .and_where(unmaterialized().or(not_done_or_committed()));

    let with_clause = WithClause::new()
        .cte(
            CommonTableExpression::new()
                .query(done_runs)
                .table_name(TransactionsWithDoneRuns)
                .to_owned(),
        )
        .cte(
            CommonTableExpression::new()
                .query(pending_reqs)
                .table_name(TransactionsWithPendingReqs)
                .to_owned(),
        )
        .to_owned();

    Query::select()
        .column((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::Id,
        ))
        .from(LogicalFunctionRunDBIden::Table)
        .join_as(
            JoinType::Join,
            TransactionsWithDoneRuns,
            TransactionsWithDoneRuns,
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TransactionId,
            ))
            .equals((
                TransactionsWithDoneRuns,
                LogicalFunctionRunDBIden::TransactionId,
            )),
        )
        .join_as(
            JoinType::LeftJoin,
            TransactionsWithPendingReqs,
            TransactionsWithPendingReqs,
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TransactionId,
            ))
            .equals((
                TransactionsWithPendingReqs,
                LogicalFunctionRunDBIden::TransactionId,
            )),
        )
        .and_where(
            Expr::col((
                TransactionsWithPendingReqs,
                LogicalFunctionRunDBIden::TransactionId,
            ))
            .is_null(),
        )
        .take()
        .with(with_clause)
}

/// Patch-agnostic: patch resolution happens at mint time, not here.
pub(crate) fn fetch_lfrs_to_execute_query(at: &AtTime, limit: u64) -> SelectStatement {
    let pending_input = correlated_chain(
        build_chain(Visibility::At(at)),
        unmaterialized().or(not_done_or_committed()),
    );
    let mut query = Query::select()
        .expr(Expr::col((LogicalFunctionRunDBIden::Table, Asterisk)))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::Status,
            ))
            .is_in([
                LogicalFunctionRunStatus::Scheduled.as_sea_expr(),
                LogicalFunctionRunStatus::ReScheduled.as_sea_expr(),
            ]),
        )
        .and_where(Expr::exists(pending_input).not())
        .order_by(
            (
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::TriggeredOn,
            ),
            Order::Asc,
        )
        .limit(limit)
        .take();
    claim::<LogicalFunctionRunDB>(&mut query);
    query
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use sea_query::PostgresQueryBuilder;
    use td_common::datetime::Timestamp;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::logical::{
        LogicalFunctionRunInputInsertDB, LogicalFunctionRunStatusUpdateDB,
        LogicalTableDataVersionInsertDB,
    };
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, DependencyPos, LogicalFunctionRunInputId,
        LogicalTableDataVersionId, ReuseFrozen, TableNameDto, TransactionKey, UserId, VersionPos,
    };
    use td_objects::types::composed::TableDependencyDto;

    /// A two-run plan whose consumer requires the producer's in-plan output
    /// before anything has materialized. That is not a block; the producer
    /// failing is.
    #[td_test::test(db)]
    async fn test_blocked_ids_same_plan_unmaterialized_input(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("c_0")?, &UserId::admin())
            .await?;
        let register = |name: &str, dependencies, tables| -> Result<FunctionRegister, TdError> {
            Ok(FunctionRegister {
                child_functions: None,
                name: name.try_into()?,
                description: "d".try_into()?,
                bundle_id: BundleId::default(),
                snippet: "s".try_into()?,
                decorator: Decorator::Publisher,
                connector: None,
                connection: None,
                base_path: None,
                dependencies,
                triggers: None,
                tables,
                table_types: None,
                parent_tables: None,
                runtime_values: "r".try_into()?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            })
        };
        let producer = db
            .scoped(p.id)
            .seed()
            .function(
                &collection,
                &register("producer", None, Some(vec![TableNameDto::try_from("t_0")?]))?,
            )
            .await?;
        let consumer = db
            .scoped(p.id)
            .seed()
            .function(
                &collection,
                &register(
                    "consumer",
                    Some(vec![TableDependencyDto::try_from("t_0")?]),
                    Some(vec![TableNameDto::try_from("t_1")?]),
                )?,
            )
            .await?;

        let execution = db.scoped(p.id).seed().execution(&producer).await?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        for function in [&producer, &consumer] {
            db.scoped(p.id)
                .seed()
                .physical_function_run(
                    &collection,
                    function,
                    &execution,
                    &transaction,
                    &LogicalFunctionRunStatus::Scheduled,
                )
                .await?;
        }
        let producer_lfr: LogicalFunctionRunDB = db
            .fetch_one(and!(&execution.base.id, &producer.base.function_id))
            .await?;
        let consumer_lfr: LogicalFunctionRunDB = db
            .fetch_one(and!(&execution.base.id, &consumer.base.function_id))
            .await?;
        let tables: Vec<TableDB> = db.scd2().fetch_all::<TableDB>(&producer.base.id).await?;
        let table = &tables[0];

        let ltdv = LogicalTableDataVersionInsertDB {
            id: LogicalTableDataVersionId::new(),
            collection_id: collection.id,
            execution_id: execution.base.id.clone(),
            transaction_id: transaction.base.id.clone(),
            table_id: table.base.table_id,
            producer_logical_function_run_id: Some(producer_lfr.base.id.clone()),
            function_param_pos: None,
            private: table.base.private,
            triggered_on: execution.base.triggered_on,
            triggered_by_id: execution.base.triggered_by_id,
            group_id: producer_lfr.base.group_id,
        };
        db.scoped(p.id)
            .chunked()
            .insert(std::slice::from_ref(&ltdv))
            .await?;
        let input = LogicalFunctionRunInputInsertDB {
            id: LogicalFunctionRunInputId::default(),
            collection_id: collection.id,
            execution_id: execution.base.id.clone(),
            transaction_id: transaction.base.id.clone(),
            logical_function_run_id: consumer_lfr.base.id.clone(),
            requirement_logical_table_data_version_id: Some(ltdv.id.clone()),
            requirement_input_idx: None,
            requirement_dependency_pos: Some(DependencyPos::try_from(0)?),
            requirement_version_pos: VersionPos::try_from(0)?,
        };
        db.scoped(p.id).insert(&input).await?;

        let blocked = db.logical_runs().blocked_ids(&execution.base.id).await?;
        assert!(
            blocked.is_empty(),
            "unmaterialized in-plan input must not block"
        );

        let failed = LogicalFunctionRunStatusUpdateDB {
            status: LogicalFunctionRunStatus::Failed,
        };
        db.chunked()
            .update(&failed, std::slice::from_ref(&producer_lfr.base.id))
            .await?;
        let blocked = db.logical_runs().blocked_ids(&execution.base.id).await?;
        assert_eq!(blocked, vec![consumer_lfr.base.id]);
        Ok(())
    }

    fn at() -> AtTime {
        AtTime::from(Timestamp::try_from(1_700_000_000_000_i64).unwrap())
    }

    #[test]
    fn test_fetch_ids_to_commit_query() {
        let ids = vec![
            ExecutionId::try_from("ep20260101-0000000001").unwrap(),
            ExecutionId::try_from("ep20260101-0000000002").unwrap(),
        ];
        let sql = fetch_ids_to_commit_query(&ids).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"WITH "transactions_with_done_runs" AS (SELECT "transaction_id" FROM "logical_function_runs" WHERE "execution_id" IN (202601010000000001, 202601010000000002) GROUP BY "transaction_id" HAVING SUM((CASE WHEN ("status" <> 'D') THEN 1 ELSE 0 END)) = 0) , "transactions_with_pending_reqs" AS (SELECT DISTINCT "logical_function_runs"."transaction_id" FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" LEFT JOIN LATERAL (SELECT "id" FROM "physical_table_data_versions" WHERE "physical_table_data_versions"."logical_table_data_version_id" = "logical_table_data_versions"."id" AND "physical_table_data_versions"."valid_to" = ('infinity'::timestamptz) LIMIT 1 OFFSET 0) AS "current_ptdv" ON TRUE LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" INNER JOIN "logical_function_runs" ON "logical_function_runs"."id" = "logical_function_run_inputs"."logical_function_run_id" WHERE ("logical_table_data_versions"."producer_logical_function_run_id" IS NULL OR "logical_table_data_versions"."producer_logical_function_run_id" <> "logical_function_run_inputs"."logical_function_run_id") AND "logical_function_runs"."execution_id" IN (202601010000000001, 202601010000000002) AND ("current_ptdv"."id" IS NULL OR ("producer_lfr"."id" IS NOT NULL AND "producer_lfr"."status" NOT IN ('D', 'C')))) SELECT "logical_function_runs"."id" FROM "logical_function_runs" JOIN "transactions_with_done_runs" AS "transactions_with_done_runs" ON "logical_function_runs"."transaction_id" = "transactions_with_done_runs"."transaction_id" LEFT JOIN "transactions_with_pending_reqs" AS "transactions_with_pending_reqs" ON "logical_function_runs"."transaction_id" = "transactions_with_pending_reqs"."transaction_id" WHERE "transactions_with_pending_reqs"."transaction_id" IS NULL"#
        );
    }

    #[test]
    fn test_fetch_lfrs_to_execute_query() {
        let sql = fetch_lfrs_to_execute_query(&at(), 50).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "logical_function_runs".* FROM "logical_function_runs" WHERE "logical_function_runs"."status" IN ('S', 'RS') AND (NOT EXISTS(SELECT "logical_function_run_inputs"."id" FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" LEFT JOIN LATERAL (SELECT "id" FROM "physical_table_data_versions" WHERE "physical_table_data_versions"."logical_table_data_version_id" = "logical_table_data_versions"."id" AND ("physical_table_data_versions"."valid_from" <= '2023-11-14 22:13:20.000000 +00:00' AND "physical_table_data_versions"."valid_to" > '2023-11-14 22:13:20.000000 +00:00') LIMIT 1 OFFSET 0) AS "current_ptdv" ON TRUE LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE ("logical_table_data_versions"."producer_logical_function_run_id" IS NULL OR "logical_table_data_versions"."producer_logical_function_run_id" <> "logical_function_run_inputs"."logical_function_run_id") AND "logical_function_run_inputs"."logical_function_run_id" = "logical_function_runs"."id" AND ("current_ptdv"."id" IS NULL OR ("producer_lfr"."id" IS NOT NULL AND "producer_lfr"."status" NOT IN ('D', 'C'))))) ORDER BY "logical_function_runs"."triggered_on" ASC LIMIT 50 FOR UPDATE OF "logical_function_runs" SKIP LOCKED"#
        );
    }

    #[test]
    fn test_blocked_ids_query() {
        let eid = ExecutionId::try_from("ep20260101-0000000001").unwrap();
        let sql = blocked_ids_query(&eid).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "id" FROM "logical_function_runs" WHERE "logical_function_runs"."execution_id" = 202601010000000001 AND EXISTS(SELECT "logical_function_run_inputs"."id" FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE ("logical_table_data_versions"."producer_logical_function_run_id" IS NULL OR "logical_table_data_versions"."producer_logical_function_run_id" <> "logical_function_run_inputs"."logical_function_run_id") AND "logical_function_run_inputs"."logical_function_run_id" = "logical_function_runs"."id" AND ("producer_lfr"."id" IS NOT NULL AND "producer_lfr"."status" IN ('F', 'H')))"#
        );
    }

    #[test]
    fn test_previous_lfr_query() {
        let fid = FunctionId::try_from("00000000000000000000000f00").unwrap();
        let before = at();
        let sql = previous_lfr_query(&fid, &before).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "id", "collection_id", "execution_id", "transaction_id", "function_id", "function_version_id", "trigger", "triggered_on", "triggered_by_id", "status", "group_id", "project_id" FROM "logical_function_runs" WHERE "function_id" = '00000000000000000000000f00' AND "triggered_on" < '2023-11-14 22:13:20.000000 +00:00' ORDER BY "triggered_on" DESC, "id" DESC LIMIT 1"#
        );
    }

    fn producers() -> Vec<LogicalFunctionRunId> {
        vec![LogicalFunctionRunId::try_from("fr20260101-0000000001").unwrap()]
    }

    #[test]
    fn test_downstream_one_hop_query() {
        let exclude = vec![LogicalFunctionRunId::try_from("fr20260101-0000000002").unwrap()];
        let sql = downstream_one_hop_query(&producers(), &exclude).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "logical_function_run_inputs"."logical_function_run_id" FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" WHERE "logical_table_data_versions"."producer_logical_function_run_id" IN (202601010000000001) AND "logical_function_run_inputs"."logical_function_run_id" <> "logical_table_data_versions"."producer_logical_function_run_id" AND "logical_function_run_inputs"."logical_function_run_id" NOT IN (202601010000000002)"#
        );
    }

    #[test]
    fn test_downstream_one_hop_query_without_exclusions() {
        let sql = downstream_one_hop_query(&producers(), &[]).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "logical_function_run_inputs"."logical_function_run_id" FROM "logical_function_run_inputs" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "logical_function_run_inputs"."requirement_logical_table_data_version_id" WHERE "logical_table_data_versions"."producer_logical_function_run_id" IN (202601010000000001) AND "logical_function_run_inputs"."logical_function_run_id" <> "logical_table_data_versions"."producer_logical_function_run_id""#
        );
    }
}
