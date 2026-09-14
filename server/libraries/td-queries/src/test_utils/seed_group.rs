//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use crate::QueryExecutor;
use td_database::access::InProject;
use td_database::domain::{Apiserver, Domained};
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB, GroupInsertDB};
use td_objects::types::basic::{
    AccessTokenId, Description, GroupId, GroupKind, GroupName, System, UserId,
};

pub(crate) async fn seed_group(
    mut executor: impl QueryExecutor + Domained<Domain = Apiserver, Scope = InProject>,
    name: GroupName,
    description: Description,
) -> Result<GroupDB, TdError> {
    let group_create = GroupCreate { name, description };

    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let group_db = GroupInsertDB {
        id: GroupId::default(),
        name: group_create.name,
        description: group_create.description,
        created_on: request_context.time.into(),
        created_by_id: request_context.user_id.into(),
        modified_on: request_context.time.into(),
        modified_by_id: request_context.user_id.into(),
        system: System::FALSE,
        kind: GroupKind::Custom,
    };

    // Runs on a project-scoped connection; the field-less `GroupDB` defaults
    // its `project_id` from that scope.
    executor.reuse().insert(&group_db).await?;
    Ok(executor.reuse().fetch_one(&group_db.id).await?)
}

#[cfg(test)]
pub mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::pool::TdPool;
    use td_objects::dxo::group::GroupDB;
    use td_objects::types::basic::{Description, GroupName};

    #[td_test::test(db)]
    async fn test_seed_group(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let group = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("test_group").unwrap(),
                Description::try_from("test group description").unwrap(),
            )
            .await
            .unwrap();

        let found = db
            .fetch_one::<GroupDB>(&GroupName::try_from("test_group").unwrap())
            .await
            .unwrap();
        assert_eq!(group.base.id, found.base.id);
        assert_eq!(group.base.name, found.base.name);
        assert_eq!(group.base.description, found.base.description);
        assert_eq!(group.base.created_on, found.base.created_on);
        assert_eq!(group.base.created_by_id, found.base.created_by_id);
        assert_eq!(group.base.modified_on, found.base.modified_on);
        assert_eq!(group.base.modified_by_id, found.base.modified_by_id);
        assert_eq!(group.base.system, found.base.system);
    }

    #[td_test::test(db)]
    async fn test_default_group_present(db: TdPool) {
        // Seeding a project seeds its three system groups.
        db.seed().test_project().await.unwrap();
        let group = db
            .fetch_one::<GroupDB>(&GroupName::default_group())
            .await
            .unwrap();
        assert!(*group.base.system);
        let source = db.fetch_one::<GroupDB>(&GroupName::source()).await.unwrap();
        assert!(*source.base.system);
        let dest = db
            .fetch_one::<GroupDB>(&GroupName::destination())
            .await
            .unwrap();
        assert!(*dest.base.system);
    }
}
