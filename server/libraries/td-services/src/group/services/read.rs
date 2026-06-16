//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::group::{Group, GroupBuilder, GroupDBWithNames};
use td_objects::rest_urls::GroupParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::GroupIdName;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadGroupService,
    request = ReadRequest<GroupParam>,
    response = Group,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<GroupParam>>::extract::<RequestContext>),
        from_fn(Scope::<Project>::set),
        // List + read are open to any role with access to the project
        // (groups are referenced by name across the API).
        from_fn(Rbac::<Project, ViewProject>::check),
        from_fn(With::<ReadRequest<GroupParam>>::extract_name::<GroupParam>),
        from_fn(With::<GroupParam>::extract::<GroupIdName>),
        from_fn(By::<GroupIdName>::fetch_one::<GroupDBWithNames>),
        from_fn(With::<GroupDBWithNames>::try_convert_to::<GroupBuilder, _>),
        from_fn(With::<GroupBuilder>::build::<Group>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::group::GroupDB;
    use td_objects::types::basic::{
        AccessTokenId, Description, GroupName, ProjectId, RoleId, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_read_group_by_name(db: TdPool) -> Result<(), TdError> {
        let _ = db
            .scoped(ProjectId::sandbox())
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin()
        )
        .read(
            GroupParam::builder()
                .group(GroupIdName::from_str("analytics")?)
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        let service = ReadGroupService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;
        let found = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_read_system_group_by_name(db: TdPool) -> Result<(), TdError> {
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin()
        )
        .read(
            GroupParam::builder()
                .group(GroupIdName::from_str("default")?)
                .build()?,
        )
        .in_project(ProjectId::sandbox());

        let service = ReadGroupService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;
        assert_eq!(response.name, GroupName::default_group());
        Ok(())
    }
}
