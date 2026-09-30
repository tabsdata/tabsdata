//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::engine::{Changes, FunctionSpec, FunctionState, apply, register_version};
use crate::function::register::{RegisterFunctionError, ensure_connection_kind_matches};
use crate::function::services::update::assert_function_name_not_exists;
use crate::function::workers::register_workers;
use std::collections::{HashMap, HashSet};
use td_bridge::BridgeProviderHandle;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::bundle::BundleStatusUpdateDB;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{
    ChildFunction, DQ_SUMMARY_TRENDS_FUNCTION_SUFFIX, FunctionDB, FunctionDBWithNames,
    FunctionInsertDB, FunctionRegister, FunctionUpdate,
};
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    FunctionId, FunctionName, FunctionType, Parent, ReuseFrozen, TableName, TableNameDto,
    TableStatus, WorkerConfig, WorkerTypeDto,
};
use td_queries::Queries;

/// What the platform stamps on a function it registers on another's behalf.
/// A request cannot express either, so this is the only way a function gets a
/// class other than `Regular`, or a parent.
pub struct PlatformOwned<'a> {
    pub function_type: FunctionType,
    pub parent: &'a FunctionId,
}

pub async fn register_one(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &ActiveCollectionDB,
    register: &FunctionRegister,
    platform: Option<PlatformOwned<'_>>,
    at: LockInstant,
) -> Result<FunctionInsertDB, TdError> {
    conn.scd2()
        .assert_not_exists::<FunctionDBWithNames>(and!(&collection.id, &register.name))
        .await?;

    // Stage/checkpoint storage prefix: the collection's effective base_path (its
    // own, else the project's -- already resolved by the collections_active view).
    let mut function =
        register.to_dao(ctx, collection.id, collection.effective_base_path.clone())?;
    if let Some(platform) = platform {
        function.function_type = platform.function_type;
        function.parent_function_id = Some(Parent::from(*platform.parent));
    }
    ensure_connection_kind_matches(conn, bridge, &function, collection).await?;
    conn.scd2().open(&function, at).await?;
    claim_bundle(conn, &function).await?;

    let state = FunctionState::empty();
    let spec = FunctionSpec::from(register);
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

pub async fn update_one(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &ActiveCollectionDB,
    current: &FunctionDBWithNames,
    update: &FunctionUpdate,
    at: LockInstant,
) -> Result<FunctionInsertDB, TdError> {
    assert_function_name_not_exists(conn, &collection.id, current, update).await?;

    let previous: FunctionDB = conn.timeline().fetch_one(&current.base.base.id).await?;
    let function = update.to_dao(&previous, ctx)?;
    ensure_connection_kind_matches(conn, bridge, &function, collection).await?;
    conn.scd2().open(&function, at).await?;
    claim_bundle(conn, &function).await?;

    let previous_version_id = Some(current.base.base.id.clone());
    let state = FunctionState::load(conn, &function.function_id).await?;
    let spec = FunctionSpec::from(update);
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

#[allow(clippy::too_many_arguments)]
async fn ensure_trend_table_is_adoptable(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    collection: &ActiveCollectionDB,
    table: &TableNameDto,
    reuse_frozen_tables: ReuseFrozen,
) -> Result<(), TdError> {
    if *reuse_frozen_tables {
        return Ok(());
    }
    let table_name = TableName::try_from(table.clone())?;
    let frozen: Vec<TableDB> = conn
        .scd2()
        .fetch_all(and!(
            &collection.id,
            table_name.clone(),
            TableStatus::Frozen
        ))
        .await?;
    if !frozen.is_empty() {
        Err(RegisterFunctionError::TrendChildTableFrozen(
            table_name,
            collection.name.clone(),
        ))?
    }
    Ok(())
}

pub async fn reconcile_dq_summary_trend_children(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &ActiveCollectionDB,
    owner: &FunctionInsertDB,
    desired: &Option<Vec<ChildFunction>>,
    reuse_frozen_tables: ReuseFrozen,
    at: LockInstant,
) -> Result<(), TdError> {
    let desired: &[ChildFunction] = desired.as_deref().unwrap_or(&[]);
    let mut declared_children: Vec<(FunctionName, TableNameDto, &ChildFunction)> =
        Vec::with_capacity(desired.len());
    for child in desired {
        if !matches!(child.function_type, FunctionType::DQSummaryTrends) {
            Err(RegisterFunctionError::ChildFunctionTypeNotAllowed(
                child.function_type.clone(),
            ))?
        }
        let table = child.output_table().ok_or_else(|| {
            RegisterFunctionError::ChildFunctionMustDeclareOneTable(child.function_type.clone())
        })?;
        let name = child.platform_name(table).ok_or_else(|| {
            RegisterFunctionError::ChildFunctionNameTooLong(
                table.clone(),
                DQ_SUMMARY_TRENDS_FUNCTION_SUFFIX,
            )
        })?;
        declared_children.push((name, table.clone(), child));
    }

    let parent = Some(Parent::from(owner.function_id));
    let existing: Vec<FunctionDBWithNames> =
        conn.scd2().fetch_all(and!(&parent, &collection.id)).await?;

    let declared: HashSet<&FunctionName> =
        declared_children.iter().map(|(name, _, _)| name).collect();
    for child in &existing {
        if !declared.contains(&child.base.base.name) {
            delete_one(conn, ctx, &child.base.base, at).await?;
        }
    }

    let by_name: HashMap<&FunctionName, &FunctionDBWithNames> =
        existing.iter().map(|f| (&f.base.base.name, f)).collect();
    for (name, table, child) in &declared_children {
        match by_name.get(name) {
            Some(current) => {
                ensure_trend_table_is_adoptable(conn, collection, table, reuse_frozen_tables)
                    .await?;
                let update = child.to_update(name, reuse_frozen_tables, owner.enabled.clone())?;
                update_one(conn, ctx, bridge, collection, current, &update, at).await?;
            }
            None => {
                let taken: Vec<FunctionDBWithNames> =
                    conn.scd2().fetch_all(and!(&collection.id, name)).await?;
                if !taken.is_empty() {
                    Err(RegisterFunctionError::ChildFunctionNameTaken(
                        table.clone(),
                        name.clone(),
                        collection.name.clone(),
                    ))?
                }
                ensure_trend_table_is_adoptable(conn, collection, table, reuse_frozen_tables)
                    .await?;
                let register =
                    child.to_register(name, reuse_frozen_tables, owner.enabled.clone())?;
                let platform = PlatformOwned {
                    function_type: child.function_type.clone(),
                    parent: &owner.function_id,
                };
                register_one(conn, ctx, bridge, collection, &register, Some(platform), at).await?;
            }
        }
    }
    Ok(())
}
