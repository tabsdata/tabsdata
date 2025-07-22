//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{td_error, TdError};
use td_objects::crudl::{handle_sql_err, RequestContext};
use td_objects::sql::cte::CteQueries;
use td_objects::sql::DerefQueries;
use td_objects::types::basic::{
    CollectionName, FunctionStatus, FunctionVersionId, TableName, TableStatus, TableVersionId,
    TriggerStatus, TriggerVersionId,
};
use td_objects::types::dependency::DependencyDB;
use td_objects::types::function::{FunctionDB, FunctionDBBuilder};
use td_objects::types::table::{TableDB, TableDBBuilder};
use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
enum DeleteTableError {
    #[error("Table '{0}' exists in collection '{1}' but it is not in frozen state: {2}")]
    TableNotFrozen(TableName, CollectionName, String) = 0,
}

pub async fn build_frozen_function_versions_dependencies<Q: DerefQueries>(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<Q>,
    Input(request_context): Input<RequestContext>,
    Input(dependencies): Input<Vec<DependencyDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    // Dependant function versions
    // TODO We can avoid this query by creating a view with dependency versions and dependency functions.
    let dependant_versions_found = if dependencies.is_empty() {
        Vec::new()
    } else {
        // TODO this is not getting chunked. If there are too many we can have issues.
        let function_status = [&FunctionStatus::Active];
        let function_versions_lookup: Vec<_> =
            dependencies.iter().map(|d| d.function_id()).collect();
        let function_versions_found: Vec<FunctionDB> = queries
            .find_versions_at::<FunctionDB>(
                None,
                Some(&function_status),
                &function_versions_lookup,
            )?
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
        function_versions_found
    };

    let frozen_versions = dependant_versions_found
        .iter()
        .map(|v| {
            FunctionDBBuilder::try_from((request_context.deref(), v.to_builder()))?
                .id(FunctionVersionId::default())
                .status(FunctionStatus::Frozen)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(frozen_versions)
}

pub async fn build_deleted_trigger_versions(
    Input(triggers): Input<Vec<TriggerDB>>,
    Input(request_context): Input<RequestContext>,
) -> Result<Vec<TriggerDB>, TdError> {
    let deleted_versions = triggers
        .iter()
        .map(|t| {
            TriggerDBBuilder::try_from((request_context.deref(), t.to_builder()))?
                .id(TriggerVersionId::default())
                .status(TriggerStatus::Deleted)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(deleted_versions)
}

pub async fn build_deleted_table_version(
    Input(collection_name): Input<CollectionName>,
    Input(existing_table_version): Input<TableDB>,
    Input(builder): Input<TableDBBuilder>,
) -> Result<TableDB, TdError> {
    if !matches!(existing_table_version.status(), TableStatus::Frozen) {
        Err(DeleteTableError::TableNotFrozen(
            existing_table_version.name().clone(),
            collection_name.deref().clone(),
            existing_table_version.status().to_string(),
        ))?
    }

    let deleted_version = builder
        .deref()
        .clone()
        .id(TableVersionId::default())
        // We use the function version id of the function that was generated when deleting the table
        // for all deleted tables.
        .status(TableStatus::Deleted)
        .build()?;
    Ok(deleted_version)
}
