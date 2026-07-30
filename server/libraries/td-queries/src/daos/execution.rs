//
// Copyright 2026 Tabsdata Inc.
//

//! Specialized execution queries that use JOINs instead of collecting
//! intermediate Vecs.

use crate::QueryExecutor;
use sea_query::{Alias, Expr, ExprTrait, JoinType, Order, Query};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::execution::Execution;
use td_objects::dxo::logical::{
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalTableDataVersionDBIden,
    LogicalTableDataVersionPointerDBIden,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBIden;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{FunctionVersionId, PhysicalTableDataVersionId};

use ta_objects::{Dao, ListDto};

query_handle! {
    /// Entry trait providing [`executions`](ExecutionsEntry::executions) on
    /// every [`QueryExecutor`](crate::QueryExecutor). Returns an
    /// [`Executions`] handle.
    pub trait ExecutionsEntry {
        fn executions(self) -> Executions<Self> {
            Executions { executor: self }
        }
    }
}

type DaoOf = <Execution as ListDto>::Dao;
type Iden = <DaoOf as ta_objects::TypedColumns>::Iden;

/// `executions`-table operations. Reach via
/// [`ExecutionsEntry::executions`]:
///
/// ```ignore
/// conn.executions().list_by_table_data_version(&tdv, &params).await?;
/// ```
pub struct Executions<E> {
    pub(crate) executor: E,
}

impl<E> Executions<E>
where
    E: QueryExecutor,
{
    /// List executions that used a specific table data version as input.
    ///
    /// Joins `executions__with_status` with `function_requirements` on
    /// `execution_id`, filtering by `requirement_table_data_version_id`.
    /// Pagination is applied at the SQL level — no intermediate Vec.
    pub async fn list_by_table_data_version(
        self,
        tdv_id: &Option<PhysicalTableDataVersionId>,
        params: &ListQueryParams<Execution>,
    ) -> Result<Vec<Execution>, SqlError> {
        let base = list_by_tdv_query(tdv_id);
        let mut query = params.apply_to(base);
        let daos: Vec<DaoOf> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<DaoOf>)?;
        daos.iter()
            .map(|dao| Execution::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    /// List executions that have requirements for a specific function version.
    ///
    /// Joins `executions__with_status` with `function_requirements` on
    /// `execution_id`, filtering by `function_version_id`.
    pub async fn list_by_function_version(
        self,
        fv_id: &FunctionVersionId,
        params: &ListQueryParams<Execution>,
    ) -> Result<Vec<Execution>, SqlError> {
        let base = list_by_fv_query(fv_id);
        let mut query = params.apply_to(base);
        let daos: Vec<DaoOf> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<DaoOf>)?;
        daos.iter()
            .map(|dao| Execution::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}

/// List executions consuming a specific TDV as input. Uses an EXISTS
/// subquery to walk the logical chain `FFR_input → LTDV → latest pointer
/// Bound to tdv_id` without aliasing `executions` columns into the
/// outer scope (avoids `ambiguous column name` on the list params'
/// ORDER BY).
pub(crate) fn list_by_tdv_query(
    tdv_id: &Option<PhysicalTableDataVersionId>,
) -> sea_query::SelectStatement {
    let lp = Alias::new("lp");
    let latest_id_subq = Query::select()
        .column(LogicalTableDataVersionPointerDBIden::Id)
        .from(LogicalTableDataVersionPointerDBIden::Table)
        .and_where(
            Expr::col((
                LogicalTableDataVersionPointerDBIden::Table,
                LogicalTableDataVersionPointerDBIden::LogicalTableDataVersionId,
            ))
            .equals((
                LogicalTableDataVersionDBIden::Table,
                LogicalTableDataVersionDBIden::Id,
            )),
        )
        .order_by(LogicalTableDataVersionPointerDBIden::SetOn, Order::Desc)
        .order_by(LogicalTableDataVersionPointerDBIden::Id, Order::Desc)
        .limit(1)
        .to_owned();

    let exists_subq = Query::select()
        .expr(Expr::value(1))
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
        .join_as(
            JoinType::InnerJoin,
            LogicalTableDataVersionPointerDBIden::Table,
            lp.clone(),
            Expr::col((lp.clone(), LogicalTableDataVersionPointerDBIden::Id)).eq(Expr::SubQuery(
                None,
                Box::new(sea_query::SubQueryStatement::SelectStatement(
                    latest_id_subq,
                )),
            )),
        )
        .and_where(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::ExecutionId,
            ))
            .equals((Iden::Table, Iden::Id)),
        )
        .and_where(
            Expr::col((
                lp,
                LogicalTableDataVersionPointerDBIden::PhysicalTableDataVersionId,
            ))
            .eq(tdv_id.as_sea_value()),
        )
        .to_owned();

    Query::select()
        .columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .from(Iden::Table)
        .and_where(Expr::exists(exists_subq))
        .take()
}

/// List executions that ran the given function version id. A head fvid
/// matches through the LFRs minted at plan-build; a patch fvid matches
/// through the bound attempts' `function_version_patch_id` — LFRs only
/// ever carry head fvids, so the patch lives on the PFR side.
pub(crate) fn list_by_fv_query(fv_id: &FunctionVersionId) -> sea_query::SelectStatement {
    let lfr_subq = Query::select()
        .expr(Expr::value(1))
        .from(LogicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ExecutionId,
            ))
            .equals((Iden::Table, Iden::Id)),
        )
        .and_where(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::FunctionVersionId,
            ))
            .eq(fv_id.as_sea_value()),
        )
        .to_owned();
    let pfr_subq = Query::select()
        .expr(Expr::value(1))
        .from(PhysicalFunctionRunDBIden::Table)
        .and_where(
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::ExecutionId,
            ))
            .equals((Iden::Table, Iden::Id)),
        )
        .and_where(
            Expr::col((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::FunctionVersionPatchId,
            ))
            .eq(fv_id.as_sea_value()),
        )
        .to_owned();

    Query::select()
        .columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .from(Iden::Table)
        .and_where(Expr::exists(lfr_subq).or(Expr::exists(pfr_subq)))
        .take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_list_by_tdv_query_sql() {
        let tdv_id = Some(PhysicalTableDataVersionId::try_from("pdv20260101-0000000001").unwrap());
        let sql = list_by_tdv_query(&tdv_id).to_string(SqliteQueryBuilder);
        assert!(sql.contains("EXISTS"), "should use EXISTS subquery: {sql}");
        assert!(
            sql.contains(r#""logical_function_run_inputs""#),
            "should reference FFR_inputs: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_versions""#),
            "should reference LTDV identity: {sql}"
        );
        assert!(
            sql.contains(r#""logical_table_data_version_pointers""#),
            "should reference the pointer event log: {sql}"
        );
        assert!(
            sql.contains(r#""physical_table_data_version_id""#),
            "should filter on the bound TDV id: {sql}"
        );
    }

    #[test]
    fn test_list_by_tdv_query_none_sql() {
        let sql = list_by_tdv_query(&None).to_string(SqliteQueryBuilder);
        assert!(
            sql.contains("IS NULL") || sql.contains(r#"= NULL"#),
            "None tdv should filter by null: {sql}"
        );
    }

    #[test]
    fn test_list_by_fv_query_sql() {
        let fv_id = FunctionVersionId::try_from("fv20260101-0000000001").unwrap();
        let sql = list_by_fv_query(&fv_id).to_string(SqliteQueryBuilder);
        assert!(sql.contains("EXISTS"), "should use EXISTS subquery: {sql}");
        assert!(
            sql.contains(r#""logical_function_runs""#),
            "should reference logical_function_runs: {sql}"
        );
        assert!(
            sql.contains(r#""function_version_id""#),
            "should filter on LFR.function_version_id: {sql}"
        );
        assert!(
            sql.contains(r#""physical_function_runs""#),
            "should reference physical_function_runs for patch attempts: {sql}"
        );
        assert!(
            sql.contains(r#""function_version_patch_id""#),
            "should filter on PFR.function_version_patch_id: {sql}"
        );
    }
}
