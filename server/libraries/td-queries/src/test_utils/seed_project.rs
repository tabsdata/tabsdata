//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use td_database::access::InProject;
use td_database::domain::Apiserver;
use td_database::pool::scoped::{Scoped, ScopedExt};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupInsertDB, SYSTEM_GROUPS};
use td_objects::dxo::project::ProjectInsertDB;
use td_objects::dxo::project::{ActiveProjectDB, ProjectCreate};
use td_objects::types::basic::{
    AccessTokenId, BasePath, Description, GroupId, GroupName, ProjectId, ProjectName, System,
    UserId,
};

/// Seed a project together with the system groups (`default`, `sources`,
/// `destinations`) — the same set the projects CRUD seeds into a freshly-created
/// project. A collection resolves its group within its own project, so a
/// project without these cannot host a collection.
///
/// The system groups belong to the *new* project, so each is seeded through
/// `db.scoped(new_project)` — the field-less `GroupDB` defaults its `project_id`
/// from that scope, mirroring `seed_default_groups`. The `projects` row itself
/// is not RLS-scoped, so it is inserted directly on the pool.
pub(crate) async fn seed_project<E>(db: E, name: ProjectName) -> Result<ActiveProjectDB, TdError>
where
    for<'s> &'s Scoped<E, ProjectId>: Writes<Domain = Apiserver, Scope = InProject>,
{
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let project_create = ProjectCreate {
        name,
        description: Description::default(),
        base_path: BasePath::default(),
    };
    let project_db = ProjectInsertDB {
        id: ProjectId::default(),
        name: project_create.name,
        description: project_create.description,
        base_path: project_create.base_path,
        created_on: request_context.time.into(),
        created_by_id: request_context.user_id.into(),
        modified_on: request_context.time.into(),
        modified_by_id: request_context.user_id.into(),
        name_when_deleted: None,
    };

    // Everything runs through the new project's scope. The `projects` row itself
    // is not RLS-scoped, so scoping is harmless there; the field-less `GroupDB`
    // rows default their `project_id` from the scope's GUC (and RLS `WITH CHECK`
    // enforces the match), mirroring `seed_default_groups`.
    let scoped = db.in_project(project_db.id);
    scoped.insert(&project_db).await?;

    for (kind, group_name, group_description) in SYSTEM_GROUPS {
        let group = GroupInsertDB {
            id: GroupId::default(),
            name: GroupName::try_from(group_name)?,
            description: Description::try_from(group_description)?,
            created_on: request_context.time.into(),
            created_by_id: request_context.user_id.into(),
            modified_on: request_context.time.into(),
            modified_by_id: request_context.user_id.into(),
            system: System::TRUE,
            kind,
        };
        scoped.insert(&group).await?;
    }

    Ok(ActiveProjectDB::from(project_db))
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::group::GroupDB;
    use td_objects::dxo::project::ActiveProjectDB;
    use td_objects::types::basic::{GroupKind, GroupName, ProjectName};

    #[td_test::test(db)]
    async fn test_seed_project(db: TdPool) {
        let project = db
            .seed()
            .project(ProjectName::try_from("seeded").unwrap())
            .await
            .unwrap();

        let found = db.fetch_one::<ActiveProjectDB>(&project.id).await.unwrap();
        assert_eq!(found.id, project.id);
        assert_eq!(found.name, project.name);

        // Project tagging is covered by `test_collection_rls_scoped_by_project`.
        for (kind, name) in [
            (GroupKind::Default, GroupName::default_group()),
            (GroupKind::Sources, GroupName::source()),
            (GroupKind::Destinations, GroupName::destination()),
        ] {
            let group: GroupDB = db
                .scoped(project.id)
                .fetch_one::<GroupDB>(&name)
                .await
                .unwrap();
            assert_eq!(
                group.base.kind, kind,
                "system group '{name}' must carry its kind"
            );
            assert!(*group.base.system);
        }
    }
}
