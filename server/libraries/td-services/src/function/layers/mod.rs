//
// Copyright 2025 Tabs Data Inc.
//

#![allow(private_bounds, non_camel_case_types)]

use crate::function::layers::register::{
    build_dependency_versions, build_table_versions, build_trigger_versions,
};
use td_authz::Authz;
use td_error::TdError;
use td_objects::crudl::RequestContext;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::InterColl;
use td_objects::tower_service::from::{
    ConvertIntoMapService, TryIntoService, UpdateService, VecBuildService, With,
};
use td_objects::tower_service::sql::insert_vec;
use td_objects::types::dependency::{DependencyDB, DependencyDBBuilder};
use td_objects::types::function::FunctionDB;
use td_objects::types::permission::{InterCollectionAccess, InterCollectionAccessBuilder};
use td_objects::types::table::{TableDB, TableDBBuilder};
use td_objects::types::trigger::{TriggerDB, TriggerDBBuilder};
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub mod delete;
pub mod register;
pub mod update;
pub mod upload;

trait DoAuthz {
    fn do_authz() -> bool;
}

pub struct DO_AUTHZ;
impl DoAuthz for DO_AUTHZ {
    fn do_authz() -> bool {
        true
    }
}

pub struct SKIP_AUTHZ;
impl DoAuthz for SKIP_AUTHZ {
    fn do_authz() -> bool {
        false
    }
}

#[layer]
pub fn register_tables() {
    layers!(
        // Insert into table_versions(sql) current function tables status=Active.
        // Reuse table_id for tables that existed (had status=Frozen)
        from_fn(With::<FunctionDB>::convert_to::<TableDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<TableDBBuilder, _>),
        from_fn(build_table_versions),
        from_fn(insert_vec::<DaoQueries, TableDB>),
    )
}

#[layer]
pub fn register_dependencies<A: DoAuthz>() {
    layers!(
        // Build dependency_versions(sql) current function table dependencies status=Active.
        from_fn(With::<FunctionDB>::convert_to::<DependencyDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<DependencyDBBuilder, _>),
        from_fn(build_dependency_versions::<DaoQueries>),
        inter_collection_authz::<_, A, DependencyDB, _>(),
        from_fn(insert_vec::<DaoQueries, DependencyDB>),
    )
}

#[layer]
pub fn register_triggers<A: DoAuthz>() {
    layers!(
        // Insert into trigger_versions(sql) current function trigger status=Active.
        from_fn(With::<FunctionDB>::convert_to::<TriggerDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<TriggerDBBuilder, _>),
        from_fn(build_trigger_versions::<DaoQueries>),
        inter_collection_authz::<_, A, TriggerDB, _>(),
        from_fn(insert_vec::<DaoQueries, TriggerDB>),
    )
}

#[layer]
fn inter_collection_authz<A, T, E>()
where
    A: DoAuthz,
    for<'a> T: Send + Sync + 'a,
    InterCollectionAccessBuilder: for<'a> TryFrom<&'a T, Error = E>,
    for<'a> E: Into<TdError> + 'a,
{
    if A::do_authz() {
        layers!(
            // inter collection authz check
            from_fn(With::<T>::vec_convert_to::<InterCollectionAccessBuilder, _>),
            from_fn(With::<InterCollectionAccessBuilder>::vec_build::<InterCollectionAccess, _>),
            from_fn(Authz::<InterColl>::check_inter_collection),
        )
    } else {
        layers!()
    }
}
