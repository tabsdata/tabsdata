//
// Copyright 2026 Tabs Data Inc.
//

use crate::project::ProjectError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::project::{ProjectUpdate, ProjectUpdateDBBuilder};
use td_objects::tower_service::with::With;
use td_tower::extractors::Ref;

/// Validate that an update request actually changes at least one field —
/// otherwise the call is a no-op and we surface it loudly.
pub async fn update_project_validate(update: Ref<ProjectUpdate>) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() {
        return Err(ProjectError::UpdateRequestHasNothingToUpdate)?;
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
