//
// Copyright 2026 Tabs Data Inc.
//

use std::ops::Deref;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB, GroupDBBuilder};
use td_objects::types::basic::{Description, GroupId, GroupName, System};
use td_objects::{Buildable, TryUpdater};
use td_queries::ChunkedQueries;
use td_security::SYSTEM_GROUPS;
use td_tower::extractors::{MutRef, Ref};

/// Seed a freshly-created project with the system groups (`default`, `source`,
/// `destination`) — the same set the schema bootstraps into `sandbox`. A
/// collection resolves its group within its own project, so every project must
/// carry these or `collection.create` would fail to resolve its group.
///
/// The system groups carry the **well-known fixed ids** (group ids are unique
/// per project, not globally), so `GroupId::source()/destination()` are valid in
/// every project — collection-type inference keys on them.
///
/// `scope_to_created_project` runs first, binding the connection to the new
/// project (`app.project_id`), so each field-less `GroupDB` defaults its
/// `project_id` from that GUC. Bypass (`Global` scope) keeps RLS `WITH CHECK`
/// satisfied; the GUC only feeds the column default.
pub async fn seed_default_groups(
    mut conn: MutRef<DynamicConnection<'static>>,
    context: Ref<RequestContext>,
) -> Result<(), TdError> {
    let mut rows = Vec::with_capacity(SYSTEM_GROUPS.len());
    for (id, name, description) in SYSTEM_GROUPS {
        let group_create = GroupCreate::builder()
            .name(GroupName::try_from(name)?)
            .description(Description::try_from(description)?)
            .build()?;
        let group = GroupDBBuilder::try_from(&group_create)?
            .try_update_from(context.deref())?
            .id(GroupId::try_from(id)?)
            .system(System::TRUE)
            .build()?;
        rows.push(group);
    }
    conn.insert_vec::<GroupDB>(&rows).await?;
    Ok(())
}
