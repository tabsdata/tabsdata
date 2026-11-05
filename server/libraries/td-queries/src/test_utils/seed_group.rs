//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use crate::Writes;
use crate::test_utils::topology::seed_instant;
use td_database::access::{Holds, InProject, ProjectLock};
use td_database::domain::Apiserver;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::group::{GroupCreate, GroupDB};
use td_objects::types::basic::{AccessTokenId, Description, GroupName, UserId};

pub(crate) async fn seed_group(
    mut executor: impl Writes<Domain = Apiserver, Scope = InProject, Mode: Holds<ProjectLock>>,
    name: GroupName,
    description: Description,
) -> Result<GroupDB, TdError> {
    let group_create = GroupCreate { name, description };

    let request_context = RequestContext::with(AccessTokenId::default(), UserId::admin());

    let at = seed_instant();
    let group_db = group_create.to_dao(&request_context);

    // Runs on a project-scoped connection; the field-less row defaults its
    // `project_id` from that scope.
    executor.reuse().scd2().open(&group_db, at).await?;
    Ok(executor
        .reuse()
        .scd2()
        .fetch_one(&group_db.group_id)
        .await?)
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
            .scoped(p.project_id)
            .seed()
            .group(
                GroupName::try_from("test_group").unwrap(),
                Description::try_from("test group description").unwrap(),
            )
            .await
            .unwrap();

        let found = db
            .scd2()
            .fetch_one::<GroupDB>(&GroupName::try_from("test_group").unwrap())
            .await
            .unwrap();
        assert_eq!(group.group_id, found.group_id);
        assert_eq!(group.name, found.name);
        assert_eq!(group.description, found.description);
        assert_eq!(group.interval, found.interval);
        assert_eq!(group.defined_by_id, found.defined_by_id);
        assert_eq!(group.system, found.system);
    }

    #[td_test::test(db)]
    async fn test_default_group_present(db: TdPool) {
        // Seeding a project seeds its three system groups.
        db.seed().test_project().await.unwrap();
        let group = db
            .scd2()
            .fetch_one::<GroupDB>(&GroupName::default_group())
            .await
            .unwrap();
        assert!(*group.system);
        let source = db
            .scd2()
            .fetch_one::<GroupDB>(&GroupName::source())
            .await
            .unwrap();
        assert!(*source.system);
        let dest = db
            .scd2()
            .fetch_one::<GroupDB>(&GroupName::destination())
            .await
            .unwrap();
        assert!(*dest.system);
    }
}
