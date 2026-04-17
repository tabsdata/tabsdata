//
// Copyright 2026 Tabs Data Inc.
//

//! Specialized execution queries that use JOINs instead of collecting
//! intermediate Vecs.

use sea_query::{Alias, Expr, ExprTrait, JoinType, Query};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::dxo::execution::Execution;
use td_objects::dxo::function_requirement::FunctionRequirementDBIden;
use td_objects::list::params::ListQueryParams;
use td_objects::types::basic::{FunctionVersionId, TableDataVersionId};

use ta_objects::{Dao, ListDto};

type DaoOf = <Execution as ListDto>::Dao;
type Iden = <DaoOf as ta_objects::TypedColumns>::Iden;

pub trait ExecutionQueries {
    /// List executions that used a specific table data version as input.
    ///
    /// Joins `executions__with_status` with `function_requirements` on
    /// `execution_id`, filtering by `requirement_table_data_version_id`.
    /// Pagination is applied at the SQL level — no intermediate Vec.
    fn list_executions_by_table_data_version(
        self,
        tdv_id: &Option<TableDataVersionId>,
        params: &ListQueryParams<Execution>,
    ) -> impl Future<Output = Result<Vec<Execution>, SqlError>>;

    /// List executions that have requirements for a specific function version.
    ///
    /// Joins `executions__with_status` with `function_requirements` on
    /// `execution_id`, filtering by `function_version_id`.
    fn list_executions_by_function_version(
        self,
        fv_id: &FunctionVersionId,
        params: &ListQueryParams<Execution>,
    ) -> impl Future<Output = Result<Vec<Execution>, SqlError>>;
}

pub(crate) fn list_by_tdv_query(tdv_id: &Option<TableDataVersionId>) -> sea_query::SelectStatement {
    let fr = Alias::new("fr");
    Query::select()
        .columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .from(Iden::Table)
        .join_as(
            JoinType::InnerJoin,
            FunctionRequirementDBIden::Table,
            fr.clone(),
            Expr::col((Iden::Table, Iden::Id))
                .equals((fr.clone(), FunctionRequirementDBIden::ExecutionId)),
        )
        .and_where(
            Expr::col((fr, FunctionRequirementDBIden::RequirementTableDataVersionId))
                .eq(tdv_id.as_sea_value()),
        )
        .group_by_columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .take()
}

pub(crate) fn list_by_fv_query(fv_id: &FunctionVersionId) -> sea_query::SelectStatement {
    let fr = Alias::new("fr");
    Query::select()
        .columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .from(Iden::Table)
        .join_as(
            JoinType::InnerJoin,
            FunctionRequirementDBIden::Table,
            fr.clone(),
            Expr::col((Iden::Table, Iden::Id))
                .equals((fr.clone(), FunctionRequirementDBIden::ExecutionId)),
        )
        .and_where(
            Expr::col((fr, FunctionRequirementDBIden::FunctionVersionId)).eq(fv_id.as_sea_value()),
        )
        .group_by_columns(DaoOf::columns().into_iter().map(|c| (Iden::Table, c)))
        .take()
}

impl<E> ExecutionQueries for E
where
    E: SeaExecutor + Send,
{
    async fn list_executions_by_table_data_version(
        self,
        tdv_id: &Option<TableDataVersionId>,
        params: &ListQueryParams<Execution>,
    ) -> Result<Vec<Execution>, SqlError> {
        let base = list_by_tdv_query(tdv_id);
        let mut query = params.apply_to(base);
        let daos: Vec<DaoOf> = query
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<DaoOf>)?;
        daos.iter()
            .map(|dao| Execution::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    async fn list_executions_by_function_version(
        self,
        fv_id: &FunctionVersionId,
        params: &ListQueryParams<Execution>,
    ) -> Result<Vec<Execution>, SqlError> {
        let base = list_by_fv_query(fv_id);
        let mut query = params.apply_to(base);
        let daos: Vec<DaoOf> = query
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<DaoOf>)?;
        daos.iter()
            .map(|dao| Execution::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;

    #[test]
    fn test_list_by_tdv_query_sql() {
        let tdv_id = Some(TableDataVersionId::try_from("DV20260101.0000000001").unwrap());
        let sql = list_by_tdv_query(&tdv_id).to_string(SqliteQueryBuilder);
        assert!(sql.contains("INNER JOIN"), "should join: {sql}");
        assert!(
            sql.contains(r#""function_requirements""#),
            "should join function_requirements: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "should group by: {sql}");
        assert!(
            sql.contains(r#""requirement_table_data_version_id""#),
            "should filter by tdv_id: {sql}"
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
        let fv_id = FunctionVersionId::try_from("FV20260101.0000000001").unwrap();
        let sql = list_by_fv_query(&fv_id).to_string(SqliteQueryBuilder);
        assert!(sql.contains("INNER JOIN"), "should join: {sql}");
        assert!(
            sql.contains(r#""function_version_id""#),
            "should filter by fv_id: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "should group by: {sql}");
    }
}
