//
// Copyright 2026 Tabsdata Inc.
//

use crate::QueryExecutor;
use crate::daos::function_runs::chain::{ProducerLfr, producer_status_in_or_lazy};
use crate::primitives::scd2::open_interval_at;
use sea_query::{Asterisk, Expr, ExprTrait, JoinType, Order, Query, SelectStatement};
use ta_objects::Dao;
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{LogicalFunctionRunDBIden, LogicalTableDataVersionDBIden};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDBIden, PhysicalTableDataVersionDBWithNames,
    PhysicalTableDataVersionDBWithNamesIden,
};
use td_objects::types::basic::{AtTime, HasData, LogicalFunctionRunStatus, TableId};

/// LTDV-rooted table-data-version queries.
pub struct TableDataVersions<E> {
    pub(crate) executor: E,
}

impl<E> TableDataVersions<E>
where
    E: QueryExecutor,
{
    /// The newest with-data materialization of `table_id` among the LTDVs
    /// triggered at or before `triggered_on`. LTDV-rooted so replays, which
    /// never move `ltdv.triggered_on`, are reflected through the open
    /// interval; `has_data = TRUE` with `ORDER BY … LIMIT 1` skips a current
    /// no-data version and falls back to the next-newest LTDV. With
    /// `committed_only`, the producer LFR must be `Committed` or absent.
    pub async fn latest_bound_tdv_with_data_for_table(
        self,
        table_id: &TableId,
        triggered_on: &AtTime,
        committed_only: bool,
    ) -> Result<Option<PhysicalTableDataVersionDBWithNames>, SqlError> {
        latest_with_data_query(table_id, triggered_on, committed_only)
            .build_query_as()
            .fetch_optional(self.executor)
            .await
            .map_err(SqlError::on_read::<PhysicalTableDataVersionDBWithNames>)
    }
}

pub(crate) fn latest_with_data_query(
    table_id: &TableId,
    triggered_on: &AtTime,
    committed_only: bool,
) -> SelectStatement {
    let ltdv_id = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::Id,
    );
    let ltdv_triggered_on = (
        LogicalTableDataVersionDBIden::Table,
        LogicalTableDataVersionDBIden::TriggeredOn,
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
    )
    .and_where(open_interval_at((
        PhysicalTableDataVersionDBWithNamesIden::Table,
        PhysicalTableDataVersionDBIden::ValidTo,
    )))
    .and_where(
        Expr::col((
            LogicalTableDataVersionDBIden::Table,
            LogicalTableDataVersionDBIden::TableId,
        ))
        .eq(table_id.as_sea_value()),
    )
    .and_where(Expr::col(ltdv_triggered_on).lte(triggered_on.as_sea_value()))
    .and_where(
        Expr::col((
            PhysicalTableDataVersionDBWithNamesIden::Table,
            PhysicalTableDataVersionDBIden::HasData,
        ))
        .eq(HasData::TRUE.as_sea_value()),
    )
    .order_by(ltdv_triggered_on, Order::Desc)
    .order_by(ltdv_id, Order::Desc)
    .limit(1);

    if committed_only {
        q.join_as(
            JoinType::LeftJoin,
            LogicalFunctionRunDBIden::Table,
            ProducerLfr,
            Expr::col((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))
            .equals((ProducerLfr, LogicalFunctionRunDBIden::Id)),
        )
        .and_where(producer_status_in_or_lazy(&[
            LogicalFunctionRunStatus::Committed,
        ]));
    }
    q
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::PostgresQueryBuilder;
    use td_common::datetime::Timestamp;
    use td_objects::types::basic::AtTime;

    fn args() -> (TableId, AtTime) {
        let at = AtTime::from(Timestamp::try_from(1_700_000_000_000_i64).unwrap());
        (TableId::try_from("00000000000000000000000t00").unwrap(), at)
    }

    #[test]
    fn test_latest_with_data_query() {
        let (table_id, on) = args();
        let sql = latest_with_data_query(&table_id, &on, false).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_table_data_versions__with_names".* FROM "physical_table_data_versions__with_names" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "physical_table_data_versions__with_names"."logical_table_data_version_id" WHERE "physical_table_data_versions__with_names"."valid_to" = ('infinity'::timestamptz) AND "logical_table_data_versions"."table_id" = '00000000000000000000000t00' AND "logical_table_data_versions"."triggered_on" <= '2023-11-14 22:13:20.000000 +00:00' AND "physical_table_data_versions__with_names"."has_data" = TRUE ORDER BY "logical_table_data_versions"."triggered_on" DESC, "logical_table_data_versions"."id" DESC LIMIT 1"#
        );
    }

    #[test]
    fn test_latest_with_data_query_committed_only() {
        let (table_id, on) = args();
        let sql = latest_with_data_query(&table_id, &on, true).to_string(PostgresQueryBuilder);
        assert_eq!(
            sql,
            r#"SELECT "physical_table_data_versions__with_names".* FROM "physical_table_data_versions__with_names" INNER JOIN "logical_table_data_versions" ON "logical_table_data_versions"."id" = "physical_table_data_versions__with_names"."logical_table_data_version_id" LEFT JOIN "logical_function_runs" AS "producer_lfr" ON "logical_table_data_versions"."producer_logical_function_run_id" = "producer_lfr"."id" WHERE "physical_table_data_versions__with_names"."valid_to" = ('infinity'::timestamptz) AND "logical_table_data_versions"."table_id" = '00000000000000000000000t00' AND "logical_table_data_versions"."triggered_on" <= '2023-11-14 22:13:20.000000 +00:00' AND "physical_table_data_versions__with_names"."has_data" = TRUE AND ("producer_lfr"."id" IS NULL OR "producer_lfr"."status" IN ('C')) ORDER BY "logical_table_data_versions"."triggered_on" DESC, "logical_table_data_versions"."id" DESC LIMIT 1"#
        );
    }
}
