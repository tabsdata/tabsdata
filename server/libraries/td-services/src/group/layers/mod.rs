//
// Copyright 2026 Tabs Data Inc.
//

use crate::group::GroupError;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::DaoCore;
use td_objects::dxo::group::{GroupDB, GroupDBWithNames, GroupUpdate};
use td_objects::errors::SqlError;
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};

/// Rejects mutation of a row whose `system` flag is true. The seeded
/// `default` / `source` / `destination` groups are protected from
/// update and delete by this guard.
pub async fn assert_not_system(group: Ref<GroupDBWithNames>) -> Result<(), TdError> {
    if *group.system {
        Err(GroupError::SystemGroup(group.name.clone()))?
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
    mut conn: MutRef<DomainConnection<Apiserver>>,
    update: Ref<GroupUpdate>,
    current: Ref<GroupDBWithNames>,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.name
        && conn
            .live()
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
