//
// Copyright 2026 Tabsdata Inc.
//

use crate::daos::function_runs::chain::{ProducerLfr, producer_status_in_or_lazy};
use crate::primitives::chunked::{MAX_INSERT_ROWS, pending_vec_query};
use crate::primitives::scd2::{open_interval_at, settle_query};
use crate::{QueryExecutor, Serves, Writes};
use sea_query::{Asterisk, Expr, ExprTrait, JoinType, Order, Query, SelectStatement};
use sqlx::postgres::PgQueryResult;
use ta_objects::Dao;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use td_database::access::LockInstant;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::dxo::logical::{LogicalFunctionRunDBIden, LogicalTableDataVersionDBIden};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionDBIden,
    PhysicalTableDataVersionDBWithNames, PhysicalTableDataVersionDBWithNamesIden,
    PhysicalTableDataVersionInsertDB, PhysicalTableDataVersionUpdateDB,
};
use td_objects::types::basic::{
    AtTime, HasData, LogicalFunctionRunStatus, LogicalTableDataVersionId, TableId,
};

/// LTDV-rooted table-data-version queries.
pub struct TableDataVersions<E> {
    pub(crate) executor: E,
}

/// Producers whose data consumers see: committed runs, and versions with no
/// producer (minted over pre-2.0 data).
const COMMITTED: &[LogicalFunctionRunStatus] = &[LogicalFunctionRunStatus::Committed];

/// What a user asking for uncommitted data also sees: runs done but not yet
/// committed. Canceled and yanked runs' data is discarded, never shown.
const INCLUDING_UNCOMMITTED: &[LogicalFunctionRunStatus] = &[
    LogicalFunctionRunStatus::Committed,
    LogicalFunctionRunStatus::Done,
];

/// Where the newest with-data version is looked for.
enum Anchor<'a> {
    /// Logical versions of `table` triggered at or before `at`.
    At(&'a TableId, &'a AtTime),
    /// Logical versions of the same table strictly before this one, in
    /// `(triggered_on, id)` order.
    Before(&'a LogicalTableDataVersionId),
}

impl<E> TableDataVersions<E>
where
    E: QueryExecutor,
{
    /// The committed data of `table_id` a reader at `at` sees: the newest
    /// current materialization with data among the logical versions
    /// triggered at or before `at` whose producer committed.
    pub async fn committed_data_at(
        self,
        table_id: &TableId,
        at: &AtTime,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        self.fetch(with_data_query(Anchor::At(table_id, at), COMMITTED))
            .await
    }

    /// The committed data a logical version without data stands for: the
    /// newest committed data of its table before it.
    pub async fn committed_data_before(
        self,
        logical_version: &LogicalTableDataVersionId,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        self.fetch(with_data_query(Anchor::Before(logical_version), COMMITTED))
            .await
    }

    /// [`Self::committed_data_at`] that also sees data of runs done but not
    /// yet committed. For users who ask for it; never for consumers.
    pub async fn data_including_uncommitted_at(
        self,
        table_id: &TableId,
        at: &AtTime,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        self.fetch(with_data_query(
            Anchor::At(table_id, at),
            INCLUDING_UNCOMMITTED,
        ))
        .await
    }

    async fn fetch(
        self,
        mut query: SelectStatement,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        query
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalTableDataVersionDBWithNames>)
    }
}

impl<E> TableDataVersions<E> {
    /// Materializations minted pending at `at`: an empty interval that closes
    /// nothing and no read sees, until [`Self::settle`] puts each into the
    /// timeline at the instant its attempt reports.
    pub async fn pending(
        self,
        versions: &[PhysicalTableDataVersionInsertDB],
        at: LockInstant,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: SeaExecutor + Serves<PhysicalTableDataVersionInsertDB> + Writes,
    {
        let mut executor = self.executor;
        let mut result = PgQueryResult::default();
        for chunk in versions.chunks(MAX_INSERT_ROWS) {
            let r = pending_vec_query(chunk, at)?
                .build_query()
                .execute(executor.reuse())
                .await
                .map_err(|e| {
                    let values: Vec<_> = chunk
                        .iter()
                        .flat_map(|dao| dao.column_display_values())
                        .collect();
                    SqlError::on_write::<PhysicalTableDataVersionInsertDB>(e, &values)
                })?;
            result.extend(std::iter::once(r));
        }
        Ok(result)
    }

    /// Writes `update` to the matching materializations: a pending one enters
    /// the timeline at `at`, superseding its logical version's open row; an
    /// open one is updated in place.
    pub async fn settle(
        self,
        update: &PhysicalTableDataVersionUpdateDB,
        condition: impl SqlCondition<PhysicalTableDataVersionDB>,
        at: LockInstant,
    ) -> Result<PgQueryResult, SqlError>
    where
        E: SeaExecutor + Serves<PhysicalTableDataVersionUpdateDB> + Writes,
    {
        settle_query(update, condition, at)?
            .build_query()
            .execute(self.executor)
            .await
            .map_err(|e| {
                SqlError::on_write::<PhysicalTableDataVersionUpdateDB>(
                    e,
                    &update.column_display_values(),
                )
            })
    }
}

#[derive(sea_query::Iden, Clone, Copy)]
struct Reader;

fn with_data_query(anchor: Anchor<'_>, producers: &[LogicalFunctionRunStatus]) -> SelectStatement {
    let ltdv_id = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::Id,
    );
    let ltdv_triggered_on = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::TriggeredOn,
    );
    let ltdv_table_id = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::TableId,
    );
    let mut q = Query::select();
    q.expr(Expr::col((
        PhysicalTableDataVersionDBWithNamesIden::Table,
        Asterisk,
    )))
    .from(PhysicalTableDataVersionDBWithNames::TABLE)
    .join(
        JoinType::InnerJoin,
        LogicalTableDataVersionDBIden::Table,
        Expr::col(ltdv_id).equals((
            PhysicalTableDataVersionDBWithNamesIden::Table,
            PhysicalTableDataVersionDBIden::LogicalTableDataVersionId,
        )),
    );
    match anchor {
        Anchor::At(table_id, at) => {
            q.and_where(Expr::col(ltdv_table_id).eq(table_id.as_sea_expr()))
                .and_where(Expr::col(ltdv_triggered_on).lte(at.as_sea_expr()));
        }
        Anchor::Before(logical_version) => {
            q.join_as(
                JoinType::InnerJoin,
                LogicalTableDataVersionDBIden::Table,
                Reader,
                Expr::col((Reader, LogicalTableDataVersionDBIden::Id))
                    .eq(logical_version.as_sea_expr()),
            )
            .and_where(
                Expr::col(ltdv_table_id).equals((Reader, LogicalTableDataVersionDBIden::TableId)),
            )
            .and_where(
                Expr::tuple([Expr::col(ltdv_triggered_on), Expr::col(ltdv_id)]).lt(Expr::tuple([
                    Expr::col((Reader, LogicalTableDataVersionDBIden::TriggeredOn)),
                    Expr::col((Reader, LogicalTableDataVersionDBIden::Id)),
                ])),
            );
        }
    }
    q.and_where(open_interval_at((
        PhysicalTableDataVersionDBWithNamesIden::Table,
        PhysicalTableDataVersionDBIden::ValidTo,
    )))
    .and_where(
        Expr::col((
            PhysicalTableDataVersionDBWithNamesIden::Table,
            PhysicalTableDataVersionDBIden::HasData,
        ))
        .eq(HasData::TRUE.as_sea_expr()),
    )
    .join_as(
        JoinType::LeftJoin,
        LogicalFunctionRunDBIden::Table,
        ProducerLfr,
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
    )
    .and_where(producer_status_in_or_lazy(producers))
    .order_by(ltdv_triggered_on, Order::Desc)
    .order_by(ltdv_id, Order::Desc)
    .limit(1);
    q
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use td_common::datetime::Timestamp;

    fn table_id() -> TableId {
        TableId::try_from("00000000000000000000000t00").unwrap()
    }

    fn at() -> AtTime {
        AtTime::from(Timestamp::try_from(1_700_000_000_000_i64).unwrap())
    }

    #[test]
    fn test_committed_data_at_query() {
        let sql = with_data_query(Anchor::At(&table_id(), &at()), COMMITTED)
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_table_data_versions__with_names".* FROM "physical_table_data_versions__with_names" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "physical_table_data_versions__with_names"."logical_table_data_version_id" LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE "logical_table_data_versions"."table_id" = '00000000000000000000000t00' AND "logical_table_data_versions"."triggered_on" <= '2023-11-14 22:13:20.000000 +00:00' AND "physical_table_data_versions__with_names"."valid_to" = ('infinity'::timestamptz) AND "physical_table_data_versions__with_names"."has_data" = TRUE AND ("producer_lfr"."id" IS NULL OR "producer_lfr"."status" IN ('C')) ORDER BY "logical_table_data_versions"."triggered_on" DESC, "logical_table_data_versions"."id" DESC LIMIT 1"#
        );
    }

    #[test]
    fn test_committed_data_before_query() {
        let ltdv = LogicalTableDataVersionId::try_from("dv20260101-0000000001").unwrap();
        let sql = with_data_query(Anchor::Before(&ltdv), COMMITTED).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_table_data_versions__with_names".* FROM "physical_table_data_versions__with_names" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "physical_table_data_versions__with_names"."logical_table_data_version_id" INNER JOIN "logical_table_data_versions" AS "reader" ON "reader"."id" = 202601010000000001 LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE "logical_table_data_versions"."table_id" = "reader"."table_id" AND ("logical_table_data_versions"."triggered_on", "logical_table_data_versions"."id") < ("reader"."triggered_on", "reader"."id") AND "physical_table_data_versions__with_names"."valid_to" = ('infinity'::timestamptz) AND "physical_table_data_versions__with_names"."has_data" = TRUE AND ("producer_lfr"."id" IS NULL OR "producer_lfr"."status" IN ('C')) ORDER BY "logical_table_data_versions"."triggered_on" DESC, "logical_table_data_versions"."id" DESC LIMIT 1"#
        );
    }

    #[test]
    fn test_data_including_uncommitted_at_query() {
        let sql = with_data_query(Anchor::At(&table_id(), &at()), INCLUDING_UNCOMMITTED)
            .to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_table_data_versions__with_names".* FROM "physical_table_data_versions__with_names" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "physical_table_data_versions__with_names"."logical_table_data_version_id" LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE "logical_table_data_versions"."table_id" = '00000000000000000000000t00' AND "logical_table_data_versions"."triggered_on" <= '2023-11-14 22:13:20.000000 +00:00' AND "physical_table_data_versions__with_names"."valid_to" = ('infinity'::timestamptz) AND "physical_table_data_versions__with_names"."has_data" = TRUE AND ("producer_lfr"."id" IS NULL OR "producer_lfr"."status" IN ('C', 'D')) ORDER BY "logical_table_data_versions"."triggered_on" DESC, "logical_table_data_versions"."id" DESC LIMIT 1"#
        );
    }
}
