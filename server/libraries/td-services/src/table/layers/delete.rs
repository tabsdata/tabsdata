//
// Copyright 2025 Tabs Data Inc.
//

use lazy_static::lazy_static;
use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DerefQueries, FindBy, UpdateBy};
use td_objects::types::basic::{
    CollectionName, DependencyStatus, FunctionStatus, FunctionVersionId, TableName, TableStatus,
    TableVersionId,
};
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::{FunctionDB, FunctionDBBuilder, FunctionVersionDB};
use td_objects::types::table::TableVersionDB;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
pub enum DeleteTableError {
    #[error("Table '{0}' exists in collection '{1}' but it is not in frozen state: {2}")]
    TableNotFrozen(TableName, CollectionName, String) = 0,
}

lazy_static! {
    static ref ACTIVE_TABLE_STATUS: TableStatus = TableStatus::Active;
    static ref FROZEN_TABLE_STATUS: TableStatus = TableStatus::Frozen;
    static ref ACTIVE_FUNCTION_STATUS: FunctionStatus = FunctionStatus::Active;
    static ref ACTIVE_DEPENDENCY_STATUS: DependencyStatus = DependencyStatus::Active;
}

pub async fn build_frozen_function_version_table(
    Input(function_version): Input<FunctionVersionDB>,
) -> Result<FunctionVersionDB, TdError> {
    let frozen = function_version
        .to_builder()
        .id(FunctionVersionId::default())
        .status(FunctionStatus::Frozen)
        .build()?;
    Ok(frozen)
}

pub async fn build_frozen_function_versions_dependencies<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(dependencies): Input<Vec<DependencyDB>>,
) -> Result<Vec<FunctionVersionDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Dependant function versions
    // TODO We can avoid this query by creating a view with dependency versions and dependency functions.
    let dependant_versions_found = if dependencies.is_empty() {
        Vec::new()
    } else {
        // TODO this is not getting chunked. If there are too many we can have issues.
        let active_status = ACTIVE_FUNCTION_STATUS.deref();
        let function_versions_lookup: Vec<_> = dependencies
            .iter()
            .map(|d| (d.function_version_id(), active_status))
            .collect();
        let function_versions_found: Vec<FunctionVersionDB> = queries
            .find_by::<FunctionVersionDB>(&function_versions_lookup)?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
        function_versions_found
    };

    let frozen_versions = dependant_versions_found
        .iter()
        .map(|v| {
            v.to_builder()
                .id(FunctionVersionId::default())
                .status(FunctionStatus::Frozen)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(frozen_versions)
}

pub async fn update_frozen_functions<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(output_function_version): Input<FunctionVersionDB>,
    Input(dependant_function_versions): Input<Vec<FunctionVersionDB>>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let to_freeze = dependant_function_versions
        .deref()
        .iter()
        .chain([output_function_version.deref()]);
    for function_version in to_freeze {
        let function = FunctionDBBuilder::try_from(function_version)?
            .frozen(true)
            .build()?;
        queries
            .update_by::<_, FunctionDB>(&function, &(function.id()))?
            .build()
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }
    Ok(())
}

pub async fn build_deleted_table_version(
    Input(collection_name): Input<CollectionName>,
    Input(function_version): Input<FunctionVersionDB>,
    Input(existing_table_version): Input<TableVersionDB>,
) -> Result<TableVersionDB, TdError> {
    if existing_table_version.status() != FROZEN_TABLE_STATUS.deref() {
        Err(DeleteTableError::TableNotFrozen(
            existing_table_version.name().clone(),
            collection_name.deref().clone(),
            existing_table_version.status().to_string(),
        ))?
    }

    let deleted_version = existing_table_version
        .to_builder()
        .id(TableVersionId::default())
        // We use the function version id of the function that was generated when deleting the table
        // for all deleted tables.
        .function_version_id(function_version.id())
        .status(TableStatus::Deleted)
        .build()?;
    Ok(deleted_version)
}
