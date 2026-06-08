//
// Copyright 2026 Tabs Data Inc.
//

use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB, GroupDBBuilder};
use td_objects::types::basic::{Description, GroupName, ProjectId, System};
use td_objects::{Buildable, TryUpdater};
use td_queries::ChunkedQueries;
use td_tower::extractors::{MutRef, Ref};

/// Seed a freshly-created project with the system groups (`default`, `source`,
/// `destination`) — the same set the schema bootstraps into `sandbox`. A
/// collection resolves its group within its own project, so every project must
/// carry these or `collection.create` would fail to resolve its group.
///
/// The create service runs bypassed (`Global` scope), so each `GroupDB` is
/// stamped with the new project's id explicitly — the same way any cross-project
/// write sets `project_id` by hand; RLS `WITH CHECK` is satisfied under bypass.
pub async fn seed_default_groups(
    mut conn: MutRef<DynamicConnection<'static>>,
    project_id: Ref<ProjectId>,
    context: Ref<RequestContext>,
) -> Result<(), TdError> {
    let system_groups = [
        (
            GroupName::default_group(),
            "Default group — assigned to collections that do not specify a group.",
        ),
        (
            GroupName::source(),
            "Source group — assigned to ingestion collections.",
        ),
        (
            GroupName::destination(),
            "Destination group — assigned to publisher collections.",
        ),
    ];

    let mut rows = Vec::with_capacity(system_groups.len());
    for (name, description) in system_groups {
        let group_create = GroupCreate::builder()
            .name(name)
            .description(Description::try_from(description)?)
            .build()?;
        let group = GroupDBBuilder::try_from(&group_create)?
            .try_update_from(context.deref())?
            .project_id((*project_id).clone())
            .system(System::from(true))
            .build()?;
        rows.push(group);
    }
    conn.insert_vec::<GroupDB>(&rows).await?;
    Ok(())
}
