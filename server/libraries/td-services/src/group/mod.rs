//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{AccessMode, InProject};
use td_error::td_error;
use td_objects::types::basic::GroupName;

pub mod services;

#[td_error]
pub enum GroupError {
    #[error("The group [{0}] is a system group and cannot be updated or deleted")]
    SystemGroup(GroupName) = 0,

    #[error("The group [{0}] still has collections and cannot be deleted")]
    GroupHasCollections(GroupName) = 2000,
    #[error("The group [{0}] is referenced by past runs and cannot be deleted")]
    GroupHasRuns(GroupName) = 2001,
}

use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::Dao;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::group::{GroupDB, GroupDBWithNames, GroupUpdate};
use td_objects::dxo::transaction::TransactionDB;
use td_objects::errors::SqlError;
use td_queries::Queries;

/// Rejects mutation of a row whose `system` flag is true. The seeded
/// `default` / `source` / `destination` groups are protected from
/// update and delete by this guard.
pub fn assert_not_system(group: &GroupDBWithNames) -> Result<(), TdError> {
    if *group.base.base.system {
        Err(GroupError::SystemGroup(group.base.base.name.clone()))?
    }
    Ok(())
}

/// Collections (soft-deleted ones included) and transactions carry the group
/// id, and the `RESTRICT` FKs refuse the delete once any exist; each run row
/// hangs off a transaction in the same group, so these two cover them all.
pub async fn assert_group_unused(
    conn: &mut DomainConnection<Apiserver, InProject, impl AccessMode>,
    group: &GroupDBWithNames,
) -> Result<(), TdError> {
    let id = group.base.base.id;
    let name = &group.base.base.name;
    if conn.fetch_first::<CollectionDB>(&id).await?.is_some() {
        Err(GroupError::GroupHasCollections(name.clone()))?
    }
    if conn.fetch_first::<TransactionDB>(&id).await?.is_some() {
        Err(GroupError::GroupHasRuns(name.clone()))?
    }
    Ok(())
}

/// On a rename (the update carries a new, different name), reject a collision
/// with another group in the project up front with a clean "already exists"
/// error instead of the raw unique-constraint violation from the update write.
/// Renaming a group to its own current name is a no-op and is not a collision.
/// The connection is project-scoped, matching the `(project_id, name)` unique
/// index — names collide only within a project.
pub async fn assert_group_rename_available(
    conn: &mut DomainConnection<Apiserver, InProject, impl AccessMode>,
    update: &GroupUpdate,
    current: &GroupDBWithNames,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.base.base.name
        && conn.fetch_optional::<GroupDB>(new_name).await?.is_some()
    {
        Err(SqlError::EntityAlreadyExists(
            GroupDB::display_name().to_string(),
            format!(" for 'name' = '{new_name}'"),
        ))?;
    }
    Ok(())
}
