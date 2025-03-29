//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DerefQueries, SelectBy};
use td_objects::types::basic::{
    CollectionId, CollectionName, DependencyStatus, DependencyVersionId, FunctionName, TableName,
    TableStatus, TableVersionId, TriggerStatus, TriggerVersionId,
};
use td_objects::types::dependency::DependencyVersionDB;
use td_objects::types::function::{
    FunctionDB, FunctionDBWithNames, FunctionUpdate, FunctionVersionDB,
};
use td_objects::types::table::TableVersionDB;
use td_objects::types::trigger::TriggerVersionDB;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
enum UpdateFunctionError {
    #[error("Function '{0}' already exists in collection '{1}'")]
    FunctionAlreadyExists(FunctionName, CollectionName) = 0,
}

pub async fn assert_function_name_not_exists<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(collection_id): Input<CollectionId>,
    Input(collection_name): Input<CollectionName>,
    Input(function): Input<FunctionDBWithNames>,
    Input(function_update): Input<FunctionUpdate>,
) -> Result<(), TdError> {
    if function_update.name() != function.name() {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let found: Option<FunctionDB> = queries
            .select_by::<FunctionDB>(&(&*collection_id, function_update.name()))?
            .build_query_as()
            .fetch_optional(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if found.is_some() {
            Err(UpdateFunctionError::FunctionAlreadyExists(
                function_update.name().clone(),
                collection_name.deref().clone(),
            ))?
        }
    }

    Ok(())
}

pub async fn vec_dropped_table_versions(
    Input(function_version): Input<FunctionVersionDB>,
    Input(new_tables): Input<Option<Vec<TableName>>>,
    Input(table_versions): Input<Vec<TableVersionDB>>,
) -> Result<Vec<TableVersionDB>, TdError> {
    let new_tables = new_tables.as_deref().unwrap_or(&[]);

    let mut versions = vec![];
    for table_version in &*table_versions {
        if new_tables.contains(table_version.name()) {
            // Skip tables that are part of the new function as well
            continue;
        }

        let version = table_version
            .to_builder()
            .id(TableVersionId::default())
            // frozen tables are part of the new function
            .function_version_id(function_version.id())
            .status(TableStatus::frozen())
            .build()?;
        versions.push(version);
    }
    Ok(versions)
}

pub async fn vec_set_dependency_version_status_delete(
    Input(function_version): Input<FunctionVersionDB>,
    Input(dep_versions): Input<Vec<DependencyVersionDB>>,
) -> Result<Vec<DependencyVersionDB>, TdError> {
    let mut versions = vec![];
    for dep in &*dep_versions {
        let version = dep
            .to_builder()
            .id(DependencyVersionId::default())
            // deleted dependencies are part of the new function
            .function_version_id(function_version.id())
            .status(DependencyStatus::deleted())
            .build()?;
        versions.push(version);
    }
    Ok(versions)
}

pub async fn vec_set_trigger_version_status_delete(
    Input(function_version): Input<FunctionVersionDB>,
    Input(trigger_versions): Input<Vec<TriggerVersionDB>>,
) -> Result<Vec<TriggerVersionDB>, TdError> {
    let mut versions = vec![];
    for trigger in &*trigger_versions {
        let version = trigger
            .to_builder()
            .id(TriggerVersionId::default())
            // deleted triggers are part of the new function
            .function_version_id(function_version.id())
            .status(TriggerStatus::deleted())
            .build()?;
        versions.push(version);
    }
    Ok(versions)
}
