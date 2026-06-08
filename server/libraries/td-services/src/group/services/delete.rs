//
// Copyright 2026 Tabs Data Inc.
//

use crate::group::layers::assert_not_system;
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, SysAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::group::{GroupDB, GroupDBWithNames};
use td_objects::rest_urls::GroupParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{GroupId, GroupIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteGroupService,
    request = DeleteRequest<GroupParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<GroupParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<DeleteRequest<GroupParam>>::extract_name::<GroupParam>),
        from_fn(With::<GroupParam>::extract::<GroupIdName>),
        from_fn(By::<GroupIdName>::fetch_one::<GroupDBWithNames>),
        // Reject deletion of the seeded system groups via the `system`
        // flag — `default` / `source` / `destination` carry it and
        // therefore cannot be deleted.
        from_fn(assert_not_system),
        from_fn(With::<GroupDBWithNames>::extract::<GroupId>),
        from_fn(By::<GroupId>::delete::<GroupDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::GroupError;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{
        AccessTokenId, Description, GroupName, ProjectId, RoleId, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_delete_group(db: TdPool) -> Result<(), TdError> {
        let group = db
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            GroupParam::builder()
                .group(GroupIdName::from_str(&format!("~{}", group.id))?)
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        let service = DeleteGroupService::with_defaults(db.clone()).service();
        service.raw_oneshot(request).await?;

        // The user-created group is gone; the seeded system rows remain.
        let gone = db
            .live()
            .fetch_all::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert!(gone.is_empty());
        let remaining = db.live().fetch_all::<GroupDB>(()).await?;
        assert_eq!(remaining.len(), 3);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_system_group_rejected(db: TdPool) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            GroupParam::builder()
                .group(GroupIdName::from_str("source")?)
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        let service = DeleteGroupService::with_defaults(db.clone()).service();
        let res = service.raw_oneshot(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<GroupError>();
        assert!(matches!(err, GroupError::SystemGroup(_)));
        Ok(())
    }
}
