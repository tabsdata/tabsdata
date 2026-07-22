//
// Copyright 2026 Tabsdata Inc.
//

use crate::group::layers::{assert_group_rename_available, assert_not_system};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project, Rbac, Scope};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::group::{
    Group, GroupBuilder, GroupDB, GroupDBUpdate, GroupDBUpdateBuilder, GroupDBWithNames,
    GroupUpdate,
};
use td_objects::rest_urls::GroupParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{GroupId, GroupIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = UpdateGroupService,
    request = UpdateRequest<GroupParam, GroupUpdate>,
    response = Group,
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<UpdateRequest<GroupParam, GroupUpdate>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ManageProject>::check),
        from_fn(With::<UpdateRequest<GroupParam, GroupUpdate>>::extract_name::<GroupParam>),
        from_fn(With::<UpdateRequest<GroupParam, GroupUpdate>>::extract_data::<GroupUpdate>),
        from_fn(With::<GroupUpdate>::try_convert_to::<GroupDBUpdateBuilder, _>),
        from_fn(With::<RequestContext>::update::<GroupDBUpdateBuilder, _>),
        from_fn(With::<GroupDBUpdateBuilder>::build::<GroupDBUpdate>),
        from_fn(With::<GroupParam>::extract::<GroupIdName>),
        from_fn(By::<GroupIdName>::fetch_one::<GroupDBWithNames>),
        // Reject mutation of the seeded system groups via the `system`
        // flag — `default` / `source` / `destination` carry it.
        from_fn(assert_not_system),
        // A rename to a name already taken by another group in the project
        // reports a clean "already exists" error instead of the raw
        // unique-constraint violation from the update write.
        from_fn(assert_group_rename_available),
        from_fn(With::<GroupDBWithNames>::extract::<GroupId>),
        from_fn(By::<GroupId>::update::<GroupDBUpdate, GroupDB>),
        from_fn(By::<GroupId>::fetch_one::<GroupDBWithNames>),
        from_fn(With::<GroupDBWithNames>::try_convert_to::<GroupBuilder, _>),
        from_fn(With::<GroupBuilder>::build::<Group>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::GroupError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_error::assert_service_error;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, Description, GroupName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_update_group(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let update = GroupUpdate::builder()
            .name(GroupName::try_from("ml")?)
            .description(Description::try_from("ML platform group")?)
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                GroupParam::builder()
                    .group(GroupIdName::from_str("analytics")?)
                    .build()?,
                update,
            )
            .in_project(p.id);

        let service = UpdateGroupService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;

        let found = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::try_from("ml")?)
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert!(!*found.system);
        Ok(())
    }

    /// Renaming a group to a name already taken by another group in the project
    /// is rejected up front with a clean `SqlError::EntityAlreadyExists` (via
    /// the `assert_group_rename_available` pre-check) rather than the raw
    /// `UniqueViolation` from the unique index.
    #[td_test::test(db)]
    async fn test_update_rename_to_existing_name_rejected(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics").unwrap(),
                Description::try_from("Analytics tier group").unwrap(),
            )
            .await;
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("ml").unwrap(),
                Description::try_from("ML platform group").unwrap(),
            )
            .await;

        let update = GroupUpdate::builder()
            .name(GroupName::try_from("ml").unwrap())
            .description(None)
            .build()
            .unwrap();

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                GroupParam::builder()
                    .group(GroupIdName::from_str("analytics").unwrap())
                    .build()
                    .unwrap(),
                update,
            )
            .in_project(p.id);

        let service = UpdateGroupService::with_defaults(db.clone()).service();
        assert_service_error(service, request, |err| match err {
            td_objects::errors::SqlError::EntityAlreadyExists(_, _) => {}
            other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
        })
        .await;
    }

    #[td_test::test(db)]
    async fn test_update_system_group_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let update = GroupUpdate::builder()
            .name(None)
            .description(Some(Description::try_from(
                "trying to mutate a system row",
            )?))
            .build()?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                GroupParam::builder()
                    .group(GroupIdName::from_str("default")?)
                    .build()?,
                update,
            )
            .in_project(p.id);

        let service = UpdateGroupService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<GroupError>();
        assert!(matches!(err, GroupError::SystemGroup(_)));
        Ok(())
    }
}
