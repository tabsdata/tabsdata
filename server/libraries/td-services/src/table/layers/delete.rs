//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_objects::{Buildable, TryUpdater};
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBBuilder};
use td_objects::dxo::trigger::TriggerDB;
use td_objects::types::basic::{
    CollectionName, DependencyStatus, DependencyVersionId, FunctionStatus, FunctionVersionId,
    TableName, TableStatus, TableVersionId, TriggerStatus, TriggerVersionId,
};
use td_tower::extractors::Ref;

#[td_error]
enum DeleteTableError {
    #[error("Table '{0}' exists in collection '{1}' but it is not in frozen state: {2}")]
    TableNotFrozen(TableName, CollectionName, String) = 0,
}

pub async fn build_frozen_functions(
    request_context: Ref<RequestContext>,
    dependant_versions_found: Ref<Vec<FunctionDB>>,
) -> Result<Vec<FunctionDB>, TdError> {
    let frozen_versions = dependant_versions_found
        .iter()
        .map(|v| {
            v.clone()
                .into_builder()
                .try_update_from(request_context.deref())?
                .id(FunctionVersionId::new())
                .status(FunctionStatus::Frozen)
                .build()
        })
        .collect::<Result<_, _>>()?;
    Ok(frozen_versions)
}

pub async fn build_deleted_triggers(
    triggers: Ref<Vec<TriggerDB>>,
    request_context: Ref<RequestContext>,
) -> Result<Vec<TriggerDB>, TdError> {
    let deleted_versions = triggers
        .iter()
        .map(|t| {
            t.clone()
                .into_builder()
                .try_update_from(request_context.deref())?
                .id(TriggerVersionId::default())
                .status(TriggerStatus::Deleted)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(deleted_versions)
}

pub async fn build_deleted_dependencies(
    deps: Ref<Vec<DependencyDB>>,
    request_context: Ref<RequestContext>,
) -> Result<Vec<DependencyDB>, TdError> {
    let deleted_versions = deps
        .iter()
        .map(|d| {
            d.clone()
                .into_builder()
                .try_update_from(request_context.deref())?
                .id(DependencyVersionId::default())
                .status(DependencyStatus::Deleted)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(deleted_versions)
}

pub async fn build_deleted_table(
    collection_name: Ref<CollectionName>,
    existing_table_version: Ref<TableDB>,
    builder: Ref<TableDBBuilder>,
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
