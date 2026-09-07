//
// Copyright 2026 Tabsdata Inc.
//

use crate::project::ProjectError;
use td_database::access::DbScope;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::Dao;
use td_objects::dxo::project::{ActiveProjectDB, ProjectUpdate};
use td_objects::errors::SqlError;
use td_queries::Queries;

/// Validate that an update request actually changes at least one field —
/// otherwise the call is a no-op and we surface it loudly.
pub fn update_project_validate(update: &ProjectUpdate) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() {
        Err(ProjectError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

/// On a rename (the update carries a new, different name), reject a collision
/// with another project up front with a clean "already exists" error instead
/// of the raw unique-constraint violation from the update write. Renaming a
/// project to its own current name is a no-op and is not a collision.
pub async fn assert_project_rename_available<S: DbScope>(
    conn: &mut DomainConnection<Apiserver, S>,
    update: &ProjectUpdate,
    current: &ActiveProjectDB,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.name
        && conn
            .fetch_optional::<ActiveProjectDB>(new_name)
            .await?
            .is_some()
    {
        Err(SqlError::EntityAlreadyExists(
            ActiveProjectDB::display_name().to_string(),
            format!(" for 'name' = '{new_name}'"),
        ))?;
    }
    Ok(())
}
