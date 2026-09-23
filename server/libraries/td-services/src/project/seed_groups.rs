//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupInsertDB, SYSTEM_GROUPS};
use td_objects::types::basic::{Description, GroupId, GroupName, System};
use td_queries::Queries;

/// Seed a freshly-created project with the system groups (`default`, `sources`,
/// `destinations`). A collection resolves its group within its own project, so
/// every project must carry these or `collection.create` would fail to resolve
/// its group.
///
/// Their ids are minted like any other group's; `kind` is what marks them as
/// the project's system groups — collection-type inference reads it.
///
/// `scope_to_created_project` runs first, binding the connection to the new
/// project (`app.project_id`), so each field-less `GroupDB` defaults its
/// `project_id` from that GUC. Bypass (`Global` scope) keeps RLS `WITH CHECK`
/// satisfied; the GUC only feeds the column default.
pub async fn seed_default_groups(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    context: &RequestContext,
) -> Result<(), TdError> {
    let mut rows = Vec::with_capacity(SYSTEM_GROUPS.len());
    for (kind, name, description) in SYSTEM_GROUPS {
        let group = GroupInsertDB {
            id: GroupId::default(),
            name: GroupName::try_from(name)?,
            description: Description::try_from(description)?,
            created_on: context.time.into(),
            created_by_id: context.user_id.into(),
            modified_on: context.time.into(),
            modified_by_id: context.user_id.into(),
            system: System::TRUE,
            kind,
        };
        rows.push(group);
    }
    conn.chunked().insert(&rows).await?;
    Ok(())
}
