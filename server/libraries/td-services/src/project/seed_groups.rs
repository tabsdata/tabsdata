//
// Copyright 2026 Tabsdata Inc.
//

use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB};
use td_objects::types::basic::{Description, GroupId, GroupName, System};
use td_queries::ChunkedQueries;
use td_security::SYSTEM_GROUPS;

/// Seed a freshly-created project with the system groups (`default`, `source`,
/// `destination`). A collection resolves its group within its own project, so
/// every project must carry these or `collection.create` would fail to resolve
/// its group.
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
    conn: &mut DomainConnection<Apiserver>,
    context: &RequestContext,
) -> Result<(), TdError> {
    let mut rows = Vec::with_capacity(SYSTEM_GROUPS.len());
    for (id, name, description) in SYSTEM_GROUPS {
        let group_create = GroupCreate {
            name: GroupName::try_from(name)?,
            description: Description::try_from(description)?,
        };
        let group = GroupDB {
            id: GroupId::try_from(id)?,
            name: group_create.name,
            description: group_create.description,
            created_on: context.time.into(),
            created_by_id: context.user_id.into(),
            modified_on: context.time.into(),
            modified_by_id: context.user_id.into(),
            system: System::TRUE,
        };
        rows.push(group);
    }
    conn.insert_vec::<GroupDB>(&rows).await?;
    Ok(())
}
