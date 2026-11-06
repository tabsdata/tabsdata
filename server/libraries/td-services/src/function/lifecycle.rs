//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::children::child_tables;
use crate::function::declared::carried_forward;
use crate::function::engine::{
    Changes, FunctionSpec, FunctionState, apply, register_version, requested_parents,
};
use crate::function::register::ensure_connection_kind_matches;
use crate::function::services::update::assert_function_name_not_exists;
use crate::function::workers::register_workers;
use crate::tag::close_tags_of;
use crate::tag::parent::stamp_parent;
use std::collections::HashMap;
use td_bridge::BridgeProviderHandle;
use td_database::access::{Holds, InProject, ProjectLock};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::bundle::BundleStatusUpdateDB;
use td_objects::dxo::collection::CollectionDBWithNames;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{
    FunctionDB, FunctionDBWithNames, FunctionInsertDB, FunctionRegister, FunctionUpdate,
};
use td_objects::dxo::topology::TopologyInstant;
use td_objects::errors::SqlError;
use td_objects::types::basic::{
    FunctionId, FunctionType, TableNameDto, WorkerConfig, WorkerTypeDto,
};
use td_queries::Queries;

/// What the platform stamps on a child it registers on its parent's behalf.
/// A request cannot express a parent function, so this is the only way a
/// function gets one. Its tables' parents come from the parent's own
/// `parent_tables`, which is the one place a request states parenting (see
/// `requested_parents`).
pub struct ChildOf<'a> {
    pub function_type: FunctionType,
    pub parent: &'a FunctionId,
    pub parent_tables: HashMap<TableNameDto, TableNameDto>,
}

impl<'a> ChildOf<'a> {
    /// The function's type; its parent is tagged once the version is open.
    fn stamp(
        self,
        function: &mut FunctionInsertDB,
    ) -> (&'a FunctionId, HashMap<TableNameDto, TableNameDto>) {
        function.function_type = self.function_type;
        (self.parent, self.parent_tables)
    }
}

pub async fn register_one(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &CollectionDBWithNames,
    register: &FunctionRegister,
    child_of: Option<ChildOf<'_>>,
    topology_at: TopologyInstant,
) -> Result<FunctionInsertDB, TdError> {
    conn.scd2()
        .assert_not_exists::<FunctionDBWithNames>(and!(&collection.collection_id, &register.name))
        .await?;

    // Stage/checkpoint storage prefix: the collection's effective base_path (its
    // own, else the project's -- already resolved by the collections__with_names view).
    let mut function = register.to_dao(
        ctx,
        collection.collection_id,
        collection.effective_base_path.clone(),
    )?;
    let (parent, parent_tables) = child_of
        .map(|child_of| child_of.stamp(&mut function))
        .unzip();
    ensure_connection_kind_matches(conn, bridge, &function, parent, collection).await?;
    conn.scd2()
        .open(&function, topology_at)
        .await
        .map_err(SqlError::already_exists)?;
    if let Some(parent) = parent {
        stamp_parent(conn, ctx, function.function_id, *parent, topology_at).await?;
    }
    claim_bundle(conn, &function).await?;

    let state = FunctionState::empty();
    let mut spec = FunctionSpec::from(register);
    spec.parent_tables = match parent_tables {
        Some(parent_tables) => Some(parent_tables),
        None => requested_parents(
            &spec,
            &function.name,
            register.parent_tables.as_ref(),
            &child_tables(register.child_functions.as_deref()),
        )?,
    };
    register_version(
        conn,
        ctx,
        topology_at,
        collection,
        &function,
        &state,
        &spec,
        &None,
        &register.workers,
    )
    .await?;
    Ok(function)
}

#[allow(clippy::too_many_arguments)]
pub async fn update_one(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &CollectionDBWithNames,
    current: &FunctionDBWithNames,
    update: &FunctionUpdate,
    child_of: Option<ChildOf<'_>>,
    topology_at: TopologyInstant,
) -> Result<FunctionInsertDB, TdError> {
    let update = &carried_forward(conn, current, update).await?;
    assert_function_name_not_exists(conn, &collection.collection_id, current, update).await?;

    let previous: FunctionDB = conn.timeline().fetch_one(&current.id).await?;
    let mut function = update.to_dao(&previous, ctx)?;
    let (parent, parent_tables) = child_of
        .map(|child_of| child_of.stamp(&mut function))
        .unzip();
    ensure_connection_kind_matches(conn, bridge, &function, parent, collection).await?;
    conn.scd2()
        .open(&function, topology_at)
        .await
        .map_err(SqlError::already_exists)?;
    if let Some(parent) = parent {
        stamp_parent(conn, ctx, function.function_id, *parent, topology_at).await?;
    }
    claim_bundle(conn, &function).await?;

    let previous_version_id = Some(current.id.clone());
    let state = FunctionState::load(conn, &function.function_id).await?;
    let mut spec = FunctionSpec::from(update);
    spec.parent_tables = match parent_tables {
        Some(parent_tables) => Some(parent_tables),
        None => requested_parents(
            &spec,
            &function.name,
            update.parent_tables.as_ref(),
            &child_tables(update.child_functions.as_deref()),
        )?,
    };
    register_version(
        conn,
        ctx,
        topology_at,
        collection,
        &function,
        &state,
        &spec,
        &previous_version_id,
        &update.workers,
    )
    .await?;
    Ok(function)
}

/// Close one function version and everything hanging off it. User tables
/// freeze so their data stays adoptable; dependencies, triggers and workers go.
pub async fn delete_one(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    ctx: &RequestContext,
    function: &FunctionInsertDB,
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    let previous_version_id = Some(function.id.clone());
    let state = FunctionState::load(conn, &function.function_id).await?;
    let changes = Changes::for_delete(&state, ctx);
    apply(conn, &changes, topology_at).await?;
    // An empty workers map so all the previous workers get deleted.
    let empty_workers = Some(HashMap::<WorkerTypeDto, WorkerConfig>::default());
    register_workers(
        conn,
        function,
        &previous_version_id,
        &empty_workers,
        topology_at.into(),
    )
    .await?;
    close_tags_of(conn, &[function.function_id], topology_at).await?;
    conn.scd2()
        .close::<FunctionDB>(&function.function_id, topology_at)
        .await?;
    Ok(())
}

/// Mark the uploaded bundle as Registered (kept as a durable record, not deleted).
async fn claim_bundle(
    conn: &mut DomainConnection<Apiserver, InProject, impl Holds<ProjectLock>>,
    function: &FunctionInsertDB,
) -> Result<(), TdError> {
    let bundle_registered = BundleStatusUpdateDB::registered();
    conn.update::<BundleStatusUpdateDB>(&bundle_registered, function.bundle_id)
        .await?;
    Ok(())
}
