//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{InProject, LockInstant, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupInsertDB, SYSTEM_GROUPS};
use td_objects::types::basic::{Description, GroupId, GroupName, GroupVersionId, System};
use td_queries::Queries;

/// Seed a freshly-created project with the system groups (`default`, `sources`,
/// `destinations`). A collection resolves its group within its own project, so
/// every project must carry these or `collection.create` would fail to resolve
/// its group.
///
/// Their ids are minted like any other group's; `kind` is what marks them as
/// the project's system groups — collection-type inference reads it.
///
/// The connection is already in the new project (`app.project_id`), so each
/// field-less row defaults its `project_id` from that GUC. The groups open at
/// the project's creation instant, `at`.
pub async fn seed_default_groups(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    context: &RequestContext,
    at: LockInstant,
) -> Result<(), TdError> {
    let mut groups = Vec::with_capacity(SYSTEM_GROUPS.len());
    for (kind, name, description) in SYSTEM_GROUPS {
        groups.push(GroupInsertDB {
            id: GroupVersionId::default(),
            group_id: GroupId::default(),
            name: GroupName::try_from(name)?,
            description: Description::try_from(description)?,
            system: System::TRUE,
            kind,
            defined_by_id: context.user_id,
        });
    }
    conn.scd2().chunked().open(&groups, at).await?;
    Ok(())
}
