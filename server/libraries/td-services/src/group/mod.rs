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
}

use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::Dao;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::group::{GroupDB, GroupUpdate};
use td_objects::errors::SqlError;
use td_queries::Queries;

/// Rejects mutation of a row whose `system` flag is true. The seeded
/// `default` / `source` / `destination` groups are protected from
/// update and delete by this guard.
pub fn assert_not_system(group: &GroupDB) -> Result<(), TdError> {
    if *group.base.system {
        Err(GroupError::SystemGroup(group.base.name.clone()))?
    }
    Ok(())
}

/// A collection never changes group, so a group with a live collection cannot
/// go. Deleted collections and past runs keep pointing at the group's
/// identity row, which a delete leaves in place.
pub async fn assert_group_unused(
    conn: &mut DomainConnection<Apiserver, InProject, impl AccessMode>,
    group: &GroupDB,
) -> Result<(), TdError> {
    if conn
        .scd2()
        .fetch_first::<CollectionDB>(&group.base.group_id)
        .await?
        .is_some()
    {
        Err(GroupError::GroupHasCollections(group.base.name.clone()))?
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
    current: &GroupDB,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.base.name
        && conn
            .scd2()
            .fetch_optional::<GroupDB>(new_name)
            .await?
            .is_some()
    {
        Err(SqlError::EntityAlreadyExists(
            GroupDB::display_name().to_string(),
            format!(" for 'name' = '{new_name}'"),
        ))?;
    }
    Ok(())
}
