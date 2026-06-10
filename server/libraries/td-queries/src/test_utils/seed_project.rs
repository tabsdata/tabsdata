//
// Copyright 2026 Tabs Data Inc.
//

use crate::LiveEntry;
use ta_objects::{Buildable, TryUpdater};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB, GroupDBBuilder};
use td_objects::dxo::project::{
    ProjectCreate, ProjectCreateDBBuilder, ProjectDB, ProjectDBBuilder,
};
use td_objects::types::basic::{
    AccessTokenId, Description, GroupName, ProjectName, RoleId, System, UserId,
};

/// Seed a project together with the system groups (`default`, `source`,
/// `destination`) — the same set the schema bootstraps into `sandbox` and the
/// projects CRUD seeds into a freshly-created project. A collection resolves
/// its group within its own project, so a project without these cannot host a
/// collection.
///
/// The system groups belong to the *new* project, so each is seeded through
/// `db.scoped(new_project)` — the field-less `GroupDB` defaults its `project_id`
/// from that scope, mirroring `seed_default_groups`. The `projects` row itself
/// is not RLS-scoped, so it is inserted directly on the pool.
pub(crate) async fn seed_project(db: &TdPool, name: ProjectName) -> Result<ProjectDB, TdError> {
    let request_context = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
    );

    let project_create = ProjectCreate::builder()
        .name(name)
        .description(Description::default())
        .build()?;
    let project_db = ProjectCreateDBBuilder::try_from(&project_create)?
        .try_update_from(&request_context)?
        .build()?;
    db.live().insert(&project_db).await?;

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
    // Seed the system groups into the *new* project: scope each insert to it so
    // the field-less `GroupDB` defaults its `project_id` from the GUC (and RLS
    // `WITH CHECK` enforces the match).
    for (group_name, group_description) in system_groups {
        let group_create = GroupCreate::builder()
            .name(group_name)
            .description(Description::try_from(group_description)?)
            .build()?;
        let group = GroupDBBuilder::try_from(&group_create)?
            .try_update_from(&request_context)?
            .system(System::from(true))
            .build()?;
        db.scoped(project_db.id)
            .live()
            .insert::<GroupDB>(&group)
            .await?;
    }

    Ok(ProjectDBBuilder::try_from(&project_db)?.build()?)
}

#[cfg(test)]
mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::group::GroupDB;
    use td_objects::dxo::project::ProjectDB;
    use td_objects::types::basic::{GroupName, ProjectName};

    #[td_test::test(db)]
    async fn test_seed_project(db: TdPool) {
        let project = db
            .seed()
            .project(ProjectName::try_from("seeded").unwrap())
            .await
            .unwrap();

        let found = db.live().fetch_one::<ProjectDB>(&project.id).await.unwrap();
        assert_eq!(found.id, project.id);
        assert_eq!(found.name, project.name);

        // The three system groups land in the new project. `project_id` is no
        // longer a `GroupDB` field (RLS owns it via the column default); that
        // the rows are tagged with the new project — not the session-default
        // sandbox — is covered end-to-end by `test_collection_rls_scoped_by_project`.
        for name in [
            GroupName::default_group(),
            GroupName::source(),
            GroupName::destination(),
        ] {
            let group: GroupDB = db
                .scoped(project.id)
                .live()
                .fetch_one::<GroupDB>(&name)
                .await
                .unwrap();
            assert!(*group.system);
        }
    }
}
