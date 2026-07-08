//
// Copyright 2026 Tabs Data Inc.
//

use crate::project::ProjectError;
use std::ops::Deref;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::DaoCore;
use td_objects::dxo::project::{ProjectDB, ProjectUpdate, ProjectUpdateDBBuilder};
use td_objects::errors::SqlError;
use td_objects::tower_service::with::With;
use td_queries::LiveEntry;
use td_tower::extractors::{MutRef, Ref};

/// Validate that an update request actually changes at least one field —
/// otherwise the call is a no-op and we surface it loudly.
pub async fn update_project_validate(update: Ref<ProjectUpdate>) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() {
        return Err(ProjectError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

/// On a rename (the update carries a new, different name), reject a collision
/// with another project up front with a clean "already exists" error instead
/// of the raw unique-constraint violation from the update write. Renaming a
/// project to its own current name is a no-op and is not a collision.
pub async fn assert_project_rename_available(
    mut conn: MutRef<DomainConnection<Apiserver>>,
    update: Ref<ProjectUpdate>,
    current: Ref<ProjectDB>,
) -> Result<(), TdError> {
    if let Some(new_name) = &update.name
        && new_name != &current.name
        && conn
            .live()
            .fetch_optional::<ProjectDB>(new_name)
            .await?
            .is_some()
    {
        Err(SqlError::EntityAlreadyExists(
            ProjectDB::display_name().to_string(),
            format!(" for 'name' = '{new_name}'"),
        ))?;
    }
    Ok(())
}

#[allow(async_fn_in_trait)]
pub trait UpdateProjectDBBuilderUpdate {
    async fn update_project_update_db_builder(
        update: Ref<ProjectUpdate>,
        builder: Ref<ProjectUpdateDBBuilder>,
    ) -> Result<ProjectUpdateDBBuilder, TdError>;
}

impl UpdateProjectDBBuilderUpdate for With<ProjectUpdate> {
    async fn update_project_update_db_builder(
        update: Ref<ProjectUpdate>,
        builder: Ref<ProjectUpdateDBBuilder>,
    ) -> Result<ProjectUpdateDBBuilder, TdError> {
        let mut builder = builder.deref().clone();
        if let Some(name) = &update.name {
            builder.name(name);
        }
        if let Some(description) = &update.description {
            builder.description(description);
        }
        Ok(builder)
    }
}
