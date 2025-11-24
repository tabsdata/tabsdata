//
// Copyright 2025 Tabs Data Inc.
//

use crate::do_authz::{DoAuthz, inter_collection_authz};
use crate::function::layers::register::{
    build_dependency_versions, build_table_versions, build_tables_trigger_versions,
    build_trigger_versions,
};
use itertools::Itertools;
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::dependency::{DependencyDB, DependencyDBBuilder};
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table::{TableDB, TableDBBuilder};
use td_objects::dxo::trigger::{TriggerDB, TriggerDBBuilder};
use td_objects::tower_service::from::{TryIntoService, UpdateService, With};
use td_objects::tower_service::sql::insert_vec;
use td_objects::types::basic::{CollectionName, TableNameDto};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_tower::extractors::Input;
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod delete;
pub mod read;
pub mod register;
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
pub fn register_dependencies<A: DoAuthz>() {
    layers!(
        // Build dependency_versions(sql) current function table dependencies status=Active.
        from_fn(With::<FunctionDB>::try_convert_to::<DependencyDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<DependencyDBBuilder, _>),
        from_fn(build_dependency_versions),
        inter_collection_authz::<_, A, DependencyDB, _>(),
        from_fn(insert_vec::<DependencyDB>),
    )
}

#[layer]
pub fn register_triggers<A: DoAuthz>() {
    layers!(
        // Insert into trigger_versions(sql) current function trigger status=Active.
        from_fn(With::<FunctionDB>::try_convert_to::<TriggerDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<TriggerDBBuilder, _>),
        from_fn(build_trigger_versions),
        inter_collection_authz::<_, A, TriggerDB, _>(),
        from_fn(insert_vec::<TriggerDB>),
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
    Input(this_collection): Input<CollectionName>,
    Input(referenced_tables): Input<Option<Vec<T>>>,
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
