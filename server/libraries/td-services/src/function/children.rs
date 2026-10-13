//
// Copyright 2026 Tabsdata Inc.
//

use crate::function::lifecycle::{ChildOf, delete_one, register_one, update_one};
use crate::function::workers::register_workers;
use std::collections::{HashMap, HashSet};
use td_bridge::BridgeProviderHandle;
use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{ChildFunction, FunctionDB, FunctionDBWithNames, FunctionInsertDB};
use td_objects::types::basic::{
    Enabled, FunctionId, FunctionName, Parent, ReuseFrozen, TableNameDto, WorkerConfig,
    WorkerTypeDto,
};
use td_queries::Queries;

#[td_error]
pub enum ChildFunctionError {
    #[error(
        "Function '{0}' is a child function managed by its parent; it cannot be updated, patched, deleted, enabled or triggered directly"
    )]
    NotUserManaged(FunctionName) = 2000,
}

pub fn ensure_user_managed(function: &FunctionInsertDB) -> Result<(), TdError> {
    if function.parent_function_id.is_some() {
        Err(ChildFunctionError::NotUserManaged(function.name.clone()))?
    }
    Ok(())
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
    collection: &ActiveCollectionDB,
    parent: &FunctionInsertDB,
    desired: &Option<Vec<ChildFunction>>,
    parent_tables: Option<&HashMap<TableNameDto, TableNameDto>>,
    reuse_frozen_tables: ReuseFrozen,
    at: LockInstant,
) -> Result<(), TdError> {
    let Some(desired) = desired else {
        return cascade_enabled(conn, ctx, &parent.function_id, parent.enabled, at).await;
    };

    let parent_id = Some(Parent::from(parent.function_id));
    let existing: Vec<FunctionDBWithNames> = conn
        .scd2()
        .fetch_all(and!(&parent_id, &collection.id))
        .await?;

    let declared: HashSet<&FunctionName> = desired.iter().map(|child| &child.name).collect();
    for child in &existing {
        if !declared.contains(&child.base.base.name) {
            delete_one(conn, ctx, &child.base.base, at).await?;
        }
    }

    let by_name: HashMap<&FunctionName, &FunctionDBWithNames> =
        existing.iter().map(|f| (&f.base.base.name, f)).collect();
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
                    at,
                )
                .await?;
            }
            None => {
                let register = child.to_register(reuse_frozen_tables, parent.enabled)?;
                register_one(conn, ctx, bridge, collection, &register, Some(child_of), at).await?;
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
    at: LockInstant,
) -> Result<(), TdError> {
    let children: Vec<FunctionDB> = conn
        .scd2()
        .fetch_all(FunctionDB::children_of(parent))
        .await?;
    let no_workers: Option<HashMap<WorkerTypeDto, WorkerConfig>> = None;
    for child in children {
        if child.base.enabled == enabled {
            continue;
        }
        let previous_version_id = Some(child.base.id.clone());
        let updated = child.re_registered(ctx, enabled);
        conn.scd2().open(&updated, at).await?;
        register_workers(conn, &updated, &previous_version_id, &no_workers, at).await?;
    }
    Ok(())
}
