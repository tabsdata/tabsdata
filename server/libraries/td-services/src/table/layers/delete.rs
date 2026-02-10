//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::dxo::TryUpdater;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBBuilder};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{
    CollectionName, DependencyStatus, DependencyVersionId, FunctionStatus, FunctionVersionId,
    TableName, TableStatus, TableVersionId, TriggerStatus, TriggerVersionId,
};
use td_tower::extractors::Input;

#[td_error]
enum DeleteTableError {
    #[error("Table '{0}' exists in collection '{1}' but it is not in frozen state: {2}")]
    TableNotFrozen(TableName, CollectionName, String) = 0,
}

pub async fn build_frozen_functions(
    Input(request_context): Input<RequestContext>,
    Input(dependant_versions_found): Input<Vec<FunctionDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let frozen_versions = dependant_versions_found
        .iter()
        .map(|v| {
            v.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(FunctionVersionId::new())
                .status(FunctionStatus::Frozen)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(frozen_versions)
}

pub async fn build_deleted_triggers(
    Input(triggers): Input<Vec<TriggerDB>>,
    Input(request_context): Input<RequestContext>,
) -> Result<Vec<TriggerDB>, TdError> {
    let deleted_versions = triggers
        .iter()
        .map(|t| {
            t.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(TriggerVersionId::default())
                .status(TriggerStatus::Deleted)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(deleted_versions)
}

pub async fn build_deleted_dependencies(
    Input(deps): Input<Vec<DependencyDB>>,
    Input(request_context): Input<RequestContext>,
) -> Result<Vec<DependencyDB>, TdError> {
    let deleted_versions = deps
        .iter()
        .map(|d| {
            d.clone()
                .to_builder()
                .try_update_from(request_context.deref())?
                .id(DependencyVersionId::default())
                .status(DependencyStatus::Deleted)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(deleted_versions)
}

pub async fn build_deleted_table(
    Input(collection_name): Input<CollectionName>,
    Input(existing_table_version): Input<TableDB>,
    Input(builder): Input<TableDBBuilder>,
) -> Result<TableDB, TdError> {
    if !matches!(existing_table_version.status, TableStatus::Frozen) {
        Err(DeleteTableError::TableNotFrozen(
            existing_table_version.name.clone(),
            collection_name.deref().clone(),
            existing_table_version.status.to_string(),
        ))?
    }

    let deleted_version = builder
        .deref()
        .clone()
        .id(TableVersionId::default())
        .status(TableStatus::Deleted)
        .build()?;
    Ok(deleted_version)
}
