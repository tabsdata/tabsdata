//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

use crate::function::register::{
    build_dependency_versions, build_table_versions, build_tables_trigger_versions,
    build_trigger_versions, build_workers, stop_current_workers,
};
use itertools::Itertools;
use std::collections::HashMap;
use ta_services::ctx_service::ReqCtx;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::{TriggerDB, TriggerDBWithNames};
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionVersionId, ProjectId, ResetState, ReuseFrozen,
    TableNameDto, TableType, WorkerConfig, WorkerTypeDto,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_queries::ChunkedQueries;

pub mod download_bundle;
pub mod patch;
pub mod read;
pub mod register;
pub mod topology;
pub mod update;
pub mod upload;

/// Insert the function's table versions (reusing table ids for tables that
/// existed) and the downstream table-trigger updates they imply. Returns the
/// new table versions.
#[allow(clippy::too_many_arguments)]
pub async fn register_tables(
    conn: &mut DomainConnection<Apiserver>,
    msgs: &mut ReqCtx,
    request_context: &RequestContext,
    function: &FunctionDB,
    collection_id: &CollectionId,
    collection_name: &CollectionName,
    at_time: &AtTime,
    existing_tables: &Vec<TableDB>,
    new_tables: &Option<Vec<TableNameDto>>,
    new_tables_types: &Option<HashMap<TableNameDto, TableType>>,
    reuse_frozen: &ReuseFrozen,
    reset_state: &ResetState,
) -> Result<Vec<TableDB>, TdError> {
    let new_table_versions = build_table_versions(
        msgs,
        collection_id,
        collection_name,
        &function.function_id,
        existing_tables,
        new_tables,
        new_tables_types,
        function,
        request_context,
        reuse_frozen,
        reset_state,
    )
    .await?;
    conn.insert_vec::<TableDB>(&new_table_versions).await?;
    let trigger_updates = build_tables_trigger_versions(conn, at_time, &new_table_versions).await?;
    conn.insert_vec::<TriggerDB>(&trigger_updates).await?;
    Ok(new_table_versions.to_vec())
}

pub async fn register_dependencies(
    conn: &mut DomainConnection<Apiserver>,
    request_context: &RequestContext,
    function: &FunctionDB,
    collection_name: &CollectionName,
    at_time: &AtTime,
    existing_dependencies: &Vec<DependencyDB>,
    new_dependencies: &Option<Vec<TableDependencyDto>>,
) -> Result<(), TdError> {
    let versions = build_dependency_versions(
        conn,
        existing_dependencies,
        new_dependencies,
        collection_name,
        &function.function_id,
        function,
        request_context,
        at_time,
    )
    .await?;
    conn.insert_vec::<DependencyDB>(&versions).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn register_triggers(
    conn: &mut DomainConnection<Apiserver>,
    request_context: &RequestContext,
    function: &FunctionDB,
    collection_name: &CollectionName,
    at_time: &AtTime,
    existing_triggers: &Vec<TriggerDBWithNames>,
    new_tables: &Option<Vec<TableNameDto>>,
    new_triggers: &Option<Vec<TableTriggerDto>>,
    new_dependencies: &Option<Vec<TableDependencyDto>>,
) -> Result<(), TdError> {
    let versions = build_trigger_versions(
        conn,
        existing_triggers,
        new_tables,
        new_triggers,
        new_dependencies,
        collection_name,
        function,
        request_context,
        at_time,
    )
    .await?;
    conn.insert_vec::<TriggerDB>(&versions).await?;
    Ok(())
}

/// Stop the previous version's cron/stage workers and insert the new ones.
///
/// `WorkerDB` keeps `project_id` (the scheduler reads it in Global scope),
/// but it's no longer mapped from `FunctionDB`; stamp it from the request
/// scope's project.
pub async fn register_workers(
    conn: &mut DomainConnection<Apiserver>,
    function: &FunctionDB,
    project_id: &ProjectId,
    previous_function_version_id: &Option<FunctionVersionId>,
    new_workers: &Option<HashMap<WorkerTypeDto, WorkerConfig>>,
) -> Result<(), TdError> {
    stop_current_workers(conn, previous_function_version_id).await?;
    let workers = build_workers(
        conn,
        function,
        project_id,
        previous_function_version_id,
        new_workers,
    )
    .await?;
    conn.insert_vec::<WorkerDB>(&workers).await?;
    Ok(())
}

pub trait ReferencedTable {
    fn referenced_collection(&self) -> &Option<CollectionName>;

    fn referenced_table(&self) -> &TableNameDto;

    fn can_be_used_from(&self, from: &CollectionName) -> bool {
        !self.referenced_table().is_private()
            || self
                .referenced_collection()
                .as_ref()
                .map_or_else(|| true, |c| c == from)
    }
}

impl ReferencedTable for TableDependencyDto {
    fn referenced_collection(&self) -> &Option<CollectionName> {
        &self.collection
    }

    fn referenced_table(&self) -> &TableNameDto {
        &self.table
    }
}

impl ReferencedTable for TableTriggerDto {
    fn referenced_collection(&self) -> &Option<CollectionName> {
        &self.collection
    }

    fn referenced_table(&self) -> &TableNameDto {
        &self.table
    }
}

#[td_error]
pub enum PrivateTableError {
    #[error("Cannot use private tables from other collections: {0}")]
    PrivateTableCannotBeUsed(String) = 0,
}

pub fn check_private_tables<T: ReferencedTable>(
    this_collection: &CollectionName,
    referenced_tables: &Option<Vec<T>>,
) -> Result<(), TdError> {
    if let Some(referenced_tables) = referenced_tables {
        let out_of_reach_private_tables = referenced_tables
            .iter()
            .filter(|t| !t.can_be_used_from(this_collection))
            .map(|t| {
                (
                    t.referenced_collection().as_ref().unwrap(),
                    t.referenced_table(),
                )
            }) // if it cannot be used it must have a collection name.
            .collect::<Vec<_>>();
        if !out_of_reach_private_tables.is_empty() {
            let out_of_reach_private_tables = out_of_reach_private_tables
                .into_iter()
                .map(|(collection, table)| format!("'{collection}/{table}'"))
                .join(", ");
            Err(PrivateTableError::PrivateTableCannotBeUsed(
                out_of_reach_private_tables,
            ))?
        }
    }
    Ok(())
}
