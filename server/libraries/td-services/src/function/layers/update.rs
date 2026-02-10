//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::handle_sql_err;
use td_objects::dxo::function::{FunctionDBBuilder, FunctionDBWithNames, FunctionUpdate};
use td_objects::sql::DaoQueries;
use td_objects::sql::cte::VersionedQueries;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionStatus, FunctionVersionId,
};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
enum UpdateFunctionError {
    #[error("Function '{0}' already exists in collection '{1}'")]
    FunctionAlreadyExists(FunctionName, CollectionName) = 0,
}

pub async fn assert_function_name_not_exists(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(at_time): Input<AtTime>,
    Input(collection_id): Input<CollectionId>,
    Input(collection_name): Input<CollectionName>,
    Input(function): Input<FunctionDBWithNames>,
    Input(function_update): Input<FunctionUpdate>,
) -> Result<(), TdError> {
    if let Some(name) = &function_update.name
        && *name != function.name
    {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let found: Option<FunctionDBWithNames> = queries
            .select_versions_at::<{ FunctionDBWithNames::Available }, FunctionDBWithNames>(
                Some(&at_time),
                &(&*collection_id, &name),
            )?
            .build_query_as()
            .fetch_optional(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if found.is_some() {
            Err(UpdateFunctionError::FunctionAlreadyExists(
                name.clone(),
                collection_name.deref().clone(),
            ))?
        }
    }

    Ok(())
}

pub async fn update_function_db_builder(
    Input(builder): Input<FunctionDBBuilder>,
    Input(update): Input<FunctionUpdate>,
) -> Result<FunctionDBBuilder, TdError> {
    let mut builder = builder.deref().clone();
    // change the version id and set it to active
    builder.id(FunctionVersionId::new());
    builder.status(FunctionStatus::Active);

    if let Some(name) = &update.name {
        builder.name(name);
    }
    if let Some(description) = &update.description {
        builder.description(description);
    }
    if let Some(bundle_id) = &update.bundle_id {
        builder.bundle_id(bundle_id);
    }
    if let Some(snippet) = &update.snippet {
        builder.snippet(snippet);
    }
    if let Some(decorator) = &update.decorator {
        builder.decorator(decorator);
    }
    if let Some(connector) = &update.connector {
        builder.connector(connector.clone());
    }
    if let Some(runtime_values) = &update.runtime_values {
        builder.runtime_values(runtime_values);
    }
    if let Some(enabled) = &update.enabled {
        builder.enabled(enabled);
    }
    Ok(builder)
}
