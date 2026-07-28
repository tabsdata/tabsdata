//
// Copyright 2025 Tabsdata Inc.
//

use std::ops::Deref;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::function::{FunctionDBBuilder, FunctionDBWithNames, FunctionUpdate};
use td_objects::types::basic::{
    CollectionId, CollectionName, FunctionName, FunctionStatus, FunctionVersionId, Snippet, StageId,
};
use td_queries::CurrentEntry;
use td_tower::extractors::{MutRef, Ref};

#[td_error]
enum UpdateFunctionError {
    #[error("Function '{0}' already exists in collection '{1}'")]
    FunctionAlreadyExists(FunctionName, CollectionName) = 0,
}

pub async fn assert_function_name_not_exists(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    collection_id: Ref<CollectionId>,
    collection_name: Ref<CollectionName>,
    function: Ref<FunctionDBWithNames>,
    function_update: Ref<FunctionUpdate>,
) -> Result<(), TdError> {
    if let Some(name) = &function_update.name
        && *name != function.name
    {
        let found = conn
            .current()
            .state(FunctionDBWithNames::available())
            .fetch_optional(and!(*collection_id, name))
            .await?;

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
    builder: Ref<FunctionDBBuilder>,
    update: Ref<FunctionUpdate>,
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
        builder.snippet(Snippet::try_from(snippet)?);
    }
    if let Some(decorator) = &update.decorator {
        builder.decorator(decorator);
    }
    if let Some(connector) = &update.connector {
        builder.connector(connector.clone());
    }
    if let Some(connection) = &update.connection {
        builder.connection(connection.clone());
    }
    if let Some(runtime_values) = &update.runtime_values {
        builder.runtime_values(runtime_values);
    }
    if let Some(enabled) = &update.enabled {
        builder.enabled(enabled);
    }
    if let Some(base_path) = &update.base_path {
        builder.base_path(Some(base_path.clone()));
    }
    // reset-stage: fresh stage id (new stage/checkpoint dir). The stage_base_path is
    // carried forward from the previous version, so the fresh stage lands at the same
    // stamped prefix — it is not relocated by an unrelated collection base_path change.
    if *update.reset_stage {
        builder.stage_id(StageId::default());
    }
    Ok(builder)
}
