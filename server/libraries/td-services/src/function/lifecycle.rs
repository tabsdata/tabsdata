//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::children::child_tables;
use crate::function::engine::{
    Changes, FunctionSpec, FunctionState, apply, register_version, requested_parents,
};
use crate::function::register::ensure_connection_kind_matches;
use crate::function::services::update::assert_function_name_not_exists;
use crate::function::workers::register_workers;
use std::collections::HashMap;
use td_bridge::BridgeProviderHandle;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::bundle::BundleStatusUpdateDB;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{
    FunctionDB, FunctionDBWithNames, FunctionInsertDB, FunctionRegister, FunctionUpdate,
};
use td_objects::types::basic::{
    FunctionId, FunctionType, Parent, TableNameDto, WorkerConfig, WorkerTypeDto,
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

impl ChildOf<'_> {
    fn stamp(self, function: &mut FunctionInsertDB) -> HashMap<TableNameDto, TableNameDto> {
        function.function_type = self.function_type;
        function.parent_function_id = Some(Parent::from(*self.parent));
        self.parent_tables
    }
}

pub async fn register_one(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &ActiveCollectionDB,
    register: &FunctionRegister,
    child_of: Option<ChildOf<'_>>,
    at: LockInstant,
) -> Result<FunctionInsertDB, TdError> {
    conn.scd2()
        .assert_not_exists::<FunctionDBWithNames>(and!(&collection.id, &register.name))
        .await?;

    // Stage/checkpoint storage prefix: the collection's effective base_path (its
    // own, else the project's -- already resolved by the collections_active view).
    let mut function =
        register.to_dao(ctx, collection.id, collection.effective_base_path.clone())?;
    let parent_tables = child_of.map(|child_of| child_of.stamp(&mut function));
    ensure_connection_kind_matches(conn, bridge, &function, collection).await?;
    conn.scd2().open(&function, at).await?;
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
        at,
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
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &ActiveCollectionDB,
    current: &FunctionDBWithNames,
    update: &FunctionUpdate,
    child_of: Option<ChildOf<'_>>,
    at: LockInstant,
) -> Result<FunctionInsertDB, TdError> {
    assert_function_name_not_exists(conn, &collection.id, current, update).await?;

    let previous: FunctionDB = conn.timeline().fetch_one(&current.base.base.id).await?;
    let mut function = update.to_dao(&previous, ctx)?;
    let parent_tables = child_of.map(|child_of| child_of.stamp(&mut function));
    ensure_connection_kind_matches(conn, bridge, &function, collection).await?;
    conn.scd2().open(&function, at).await?;
    claim_bundle(conn, &function).await?;

    let previous_version_id = Some(current.base.base.id.clone());
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
        at,
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
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    function: &FunctionInsertDB,
    at: LockInstant,
) -> Result<(), TdError> {
    let previous_version_id = Some(function.id.clone());
    let state = FunctionState::load(conn, &function.function_id).await?;
    let changes = Changes::for_delete(&state, ctx);
    apply(conn, &changes, at).await?;
    // An empty workers map so all the previous workers get deleted.
    let empty_workers = Some(HashMap::<WorkerTypeDto, WorkerConfig>::default());
    register_workers(conn, function, &previous_version_id, &empty_workers).await?;
    conn.scd2()
        .close::<FunctionDB>(&function.function_id, at)
        .await?;
    Ok(())
}

/// Mark the uploaded bundle as Registered (kept as a durable record, not deleted).
async fn claim_bundle(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    function: &FunctionInsertDB,
) -> Result<(), TdError> {
    let bundle_registered = BundleStatusUpdateDB::registered();
    conn.update::<BundleStatusUpdateDB>(&bundle_registered, function.bundle_id)
        .await?;
    Ok(())
}
