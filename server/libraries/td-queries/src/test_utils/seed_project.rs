//
// Copyright 2026 Tabsdata Inc.
//

use crate::LiveEntry;
use crate::QueryExecutor;
use td_database::domain::{Apiserver, Domained};
use td_database::pool::scoped::{Scoped, ScopedExt};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB};
use td_objects::dxo::project::ProjectCreateDB;
use td_objects::dxo::project::{ActiveProjectDB, ProjectCreate};
use td_objects::types::basic::{
    AccessTokenId, BasePath, Description, GroupId, GroupName, ProjectId, ProjectName, System,
    UserId,
};
use td_security::SYSTEM_GROUPS;

/// Seed a project together with the system groups (`default`, `source`,
/// `destination`) — the same set the projects CRUD seeds into a freshly-created
/// project. A collection resolves its group within its own project, so a
/// project without these cannot host a collection.
///
/// The system groups belong to the *new* project, so each is seeded through
/// `db.scoped(new_project)` — the field-less `GroupDB` defaults its `project_id`
/// from that scope, mirroring `seed_default_groups`. The `projects` row itself
/// is not RLS-scoped, so it is inserted directly on the pool.
pub(crate) async fn seed_project<E>(db: E, name: ProjectName) -> Result<ActiveProjectDB, TdError>
where
    for<'s> &'s Scoped<E, ProjectId>: QueryExecutor + Domained<Domain = Apiserver>,
{
    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let project_create = ProjectCreate {
        name,
        description: Description::default(),
        base_path: BasePath::default(),
    };
    let project_db = ProjectCreateDB {
        id: ProjectId::default(),
        name: project_create.name,
        description: project_create.description,
        base_path: project_create.base_path,
        created_on: request_context.time,
        created_by_id: request_context.user_id,
        modified_on: request_context.time,
        modified_by_id: request_context.user_id,
        name_when_deleted: None,
    };

    // Everything runs through the new project's scope. The `projects` row itself
    // is not RLS-scoped, so scoping is harmless there; the field-less `GroupDB`
    // rows default their `project_id` from the scope's GUC (and RLS `WITH CHECK`
    // enforces the match), mirroring `seed_default_groups`.
    let scoped = db.in_project(project_db.id);
    scoped.live().insert(&project_db).await?;

    for (group_id, group_name, group_description) in SYSTEM_GROUPS {
        let group_create = GroupCreate {
            name: GroupName::try_from(group_name)?,
            description: Description::try_from(group_description)?,
        };
        let group = GroupDB {
            id: GroupId::try_from(group_id)?,
            name: group_create.name,
            description: group_create.description,
            created_on: request_context.time,
            created_by_id: request_context.user_id,
            modified_on: request_context.time,
            modified_by_id: request_context.user_id,
            system: System::TRUE,
        };
        scoped.live().insert::<GroupDB>(&group).await?;
    }

    Ok(ActiveProjectDB::from(project_db))
}

#[cfg(test)]
mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::group::GroupDB;
    use td_objects::dxo::project::ActiveProjectDB;
    use td_objects::types::basic::{GroupId, GroupName, ProjectName};

    #[td_test::test(db)]
    async fn test_seed_project(db: TdPool) {
        let project = db
            .seed()
            .project(ProjectName::try_from("seeded").unwrap())
            .await
            .unwrap();

        let found = db
            .live()
            .fetch_one::<ActiveProjectDB>(&project.id)
            .await
            .unwrap();
        assert_eq!(found.id, project.id);
        assert_eq!(found.name, project.name);

        // The three system groups land in the new project, carrying the
        // well-known fixed ids (group ids are unique per project). `project_id`
        // is no longer a `GroupDB` field (RLS owns it via the column default);
        // that the rows are tagged with the new project is covered end-to-end
        // by `test_collection_rls_scoped_by_project`.
        for (id, name) in [
            (GroupId::default_group(), GroupName::default_group()),
            (GroupId::source(), GroupName::source()),
            (GroupId::destination(), GroupName::destination()),
        ] {
            let group: GroupDB = db
                .scoped(project.id)
                .live()
                .fetch_one::<GroupDB>(&name)
                .await
                .unwrap();
            assert_eq!(
                group.id, id,
                "system group '{name}' must carry the fixed id"
            );
            assert!(*group.system);
        }
    }
}
