//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::layers::register::{
    build_dependency_versions, build_table_versions, build_tables_trigger_versions,
    build_trigger_versions, build_workers, stop_current_workers,
};
use itertools::Itertools;
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::{DependencyDB, DependencyDBBuilder};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBBuilder};
use td_objects::dxo::trigger::{TriggerDB, TriggerDBBuilder};
use td_objects::dxo::worker::{WorkerDB, WorkerDBBuilder};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionName, ProjectId, TableNameDto};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_queries::insert_vec;
use td_tower::extractors::Ref;
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod delete;
pub mod download_bundle;
pub mod enable;
pub mod patch;
pub mod read;
pub mod register;
pub mod topology;
pub mod update;
pub mod upload;

#[layer]
pub fn register_tables() {
    layers!(
        // Insert into table_versions(sql) current function tables status=Active.
        // Reuse table_id for tables that existed (had status=Frozen)
        from_fn(With::<FunctionDB>::try_convert_to::<TableDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<TableDBBuilder, _>),
        from_fn(build_table_versions),
        from_fn(insert_vec::<TableDB>),
        // Insert into trigger_versions(sql) downstream table triggers updates.
        from_fn(build_tables_trigger_versions),
        from_fn(insert_vec::<TriggerDB>),
    )
}

#[layer]
pub fn register_dependencies() {
    layers!(
        // Build dependency_versions(sql) current function table dependencies status=Active.
        from_fn(With::<FunctionDB>::try_convert_to::<DependencyDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<DependencyDBBuilder, _>),
        from_fn(build_dependency_versions),
        from_fn(insert_vec::<DependencyDB>),
    )
}

#[layer]
pub fn register_triggers() {
    layers!(
        // Insert into trigger_versions(sql) current function trigger status=Active.
        from_fn(With::<FunctionDB>::try_convert_to::<TriggerDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<TriggerDBBuilder, _>),
        from_fn(build_trigger_versions),
        from_fn(insert_vec::<TriggerDB>),
    )
}

#[layer]
pub fn register_workers() {
    layers!(
        // Set stopped for current workers and insert into workers(sql) new workers.
        from_fn(stop_current_workers),
        from_fn(With::<FunctionDB>::try_convert_to::<WorkerDBBuilder, _>),
        // `WorkerDB` keeps `project_id` (the scheduler reads it in Global scope),
        // but it's no longer mapped from `FunctionDB`; stamp it from the request
        // scope's `ProjectId`, on the handler for every `scope = "Project"` caller.
        from_fn(With::<ProjectId>::set::<WorkerDBBuilder>),
        from_fn(build_workers),
        from_fn(insert_vec::<WorkerDB>),
    )
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

pub async fn check_private_tables<T: ReferencedTable + Send + Sync>(
    this_collection: Ref<CollectionName>,
    referenced_tables: Ref<Option<Vec<T>>>,
) -> Result<(), TdError> {
    if let Some(referenced_tables) = referenced_tables.deref() {
        let out_of_reach_private_tables = referenced_tables
            .iter()
            .filter(|t| !t.can_be_used_from(&this_collection))
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
