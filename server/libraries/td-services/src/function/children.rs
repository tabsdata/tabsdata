//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::lifecycle::{ChildOf, delete_one, register_one, update_one};
use crate::function::workers::register_workers;
use crate::tag::parent::{children_of, parent_of};
use std::collections::{HashMap, HashSet};
use td_bridge::BridgeProviderHandle;
use td_database::access::{AccessMode, InProject, SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::collection::CollectionDBWithNames;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{ChildFunction, FunctionDB, FunctionDBWithNames, FunctionInsertDB};
use td_objects::dxo::topology::TopologyInstant;
use td_objects::types::basic::{
    Enabled, FunctionId, FunctionName, ReuseFrozen, TableNameDto, WorkerConfig, WorkerTypeDto,
};
use td_queries::Queries;

#[td_error]
pub enum ChildFunctionError {
    #[error(
        "Function '{0}' is a child function managed by its parent; it cannot be updated, patched, deleted, enabled or triggered directly"
    )]
    NotUserManaged(FunctionName) = 2000,
}

/// A function with a parent is the parent's to update, delete, enable or run.
pub fn ensure_user_managed(
    function: &FunctionInsertDB,
    parent: Option<&FunctionId>,
) -> Result<(), TdError> {
    if parent.is_some() {
        Err(ChildFunctionError::NotUserManaged(function.name.clone()))?
    }
    Ok(())
}

pub async fn ensure_user_managed_now<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    function: &FunctionInsertDB,
) -> Result<(), TdError> {
    let parent = parent_of(conn, function.function_id).await?;
    ensure_user_managed(function, parent.as_ref())
}

pub fn child_tables(children: Option<&[ChildFunction]>) -> HashSet<&TableNameDto> {
    children
        .unwrap_or_default()
        .iter()
        .flat_map(|child| child.tables.iter().flatten())
        .collect()
}

/// `None` keeps the children, which still follow the parent's `enabled`.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_children(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    bridge: &BridgeProviderHandle,
    collection: &CollectionDBWithNames,
    parent: &FunctionInsertDB,
    desired: &Option<Vec<ChildFunction>>,
    parent_tables: Option<&HashMap<TableNameDto, TableNameDto>>,
    reuse_frozen_tables: ReuseFrozen,
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    let Some(desired) = desired else {
        return cascade_enabled(conn, ctx, &parent.function_id, parent.enabled, topology_at).await;
    };

    let children: Vec<FunctionId> = children_of(conn, parent.function_id).await?;
    let existing: Vec<FunctionDBWithNames> = conn
        .scd2()
        .chunked()
        .fetch_all(&children, &collection.collection_id)
        .await?;

    let declared: HashSet<&FunctionName> = desired.iter().map(|child| &child.name).collect();
    for child in &existing {
        if !declared.contains(&child.name) {
            delete_one(conn, ctx, child, topology_at).await?;
        }
    }

    let by_name: HashMap<&FunctionName, &FunctionDBWithNames> =
        existing.iter().map(|f| (&f.name, f)).collect();
    for child in desired {
        let child_of = ChildOf {
            function_type: child.function_type.clone(),
            parent: &parent.function_id,
            parent_tables: child
                .tables
                .iter()
                .flatten()
                .filter_map(|table| {
                    let parent_table = parent_tables?.get(table)?;
                    Some((table.clone(), parent_table.clone()))
                })
                .collect(),
        };
        match by_name.get(&child.name) {
            Some(current) => {
                let update = child.to_update(reuse_frozen_tables, parent.enabled)?;
                update_one(
                    conn,
                    ctx,
                    bridge,
                    collection,
                    current,
                    &update,
                    Some(child_of),
                    topology_at,
                )
                .await?;
            }
            None => {
                let register = child.to_register(reuse_frozen_tables, parent.enabled)?;
                register_one(
                    conn,
                    ctx,
                    bridge,
                    collection,
                    &register,
                    Some(child_of),
                    topology_at,
                )
                .await?;
            }
        }
    }
    Ok(())
}

pub async fn cascade_enabled(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    parent: &FunctionId,
    enabled: Enabled,
    topology_at: TopologyInstant,
) -> Result<(), TdError> {
    let children: Vec<FunctionId> = children_of(conn, *parent).await?;
    let children: Vec<FunctionDB> = conn.scd2().chunked().fetch_all(&children, ()).await?;
    let no_workers: Option<HashMap<WorkerTypeDto, WorkerConfig>> = None;
    for child in children {
        if child.enabled == enabled {
            continue;
        }
        let previous_version_id = Some(child.id.clone());
        let updated = child.re_registered(ctx, enabled);
        conn.scd2().open(&updated, topology_at).await?;
        register_workers(
            conn,
            &updated,
            &previous_version_id,
            &no_workers,
            topology_at.into(),
        )
        .await?;
    }
    Ok(())
}
