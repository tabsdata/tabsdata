//
// Copyright 2026 Tabs Data Inc.
//

use crate::LiveEntry;
use crate::QueryExecutor;
use ta_objects::{Buildable, TryUpdater};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB, GroupDBBuilder};
use td_objects::types::basic::{AccessTokenId, Description, GroupName, RoleId, UserId};

pub(crate) async fn seed_group(
    mut executor: impl QueryExecutor,
    name: GroupName,
    description: Description,
) -> Result<GroupDB, TdError> {
    let group_create = GroupCreate::builder()
        .name(name)
        .description(description)
        .build()?;

    let request_context = RequestContext::with(
        AccessTokenId::default(),
        UserId::admin(),
        RoleId::sys_admin(),
    );

    let group_db = GroupDBBuilder::try_from(&group_create)?
        .try_update_from(&request_context)?
        .build()?;

    // Runs on a project-scoped connection; the field-less `GroupDB` defaults
    // its `project_id` from that scope.
    executor.reuse().live().insert(&group_db).await?;
    Ok(group_db)
}

#[cfg(test)]
pub mod tests {
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::group::GroupDB;
    use td_objects::types::basic::{Description, GroupName, ProjectId};

    #[td_test::test(db)]
    async fn test_seed_group(db: TdPool) {
        let group = db
            .scoped(ProjectId::sandbox())
            .seed()
            .group(
                GroupName::try_from("test_group").unwrap(),
                Description::try_from("test group description").unwrap(),
            )
            .await
            .unwrap();

        let found = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::try_from("test_group").unwrap())
            .await
            .unwrap();
        assert_eq!(group.id, found.id);
        assert_eq!(group.name, found.name);
        assert_eq!(group.description, found.description);
        assert_eq!(group.created_on, found.created_on);
        assert_eq!(group.created_by_id, found.created_by_id);
        assert_eq!(group.modified_on, found.modified_on);
        assert_eq!(group.modified_by_id, found.modified_by_id);
        assert_eq!(group.system, found.system);
    }

    #[td_test::test(db)]
    async fn test_default_group_present(db: TdPool) {
        // The three seeded system groups must be in the table after schema
        // build.
        let group = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::default_group())
            .await
            .unwrap();
        assert!(*group.system);
        let source = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::source())
            .await
            .unwrap();
        assert!(*source.system);
        let dest = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::destination())
            .await
            .unwrap();
        assert!(*dest.system);
    }
}
