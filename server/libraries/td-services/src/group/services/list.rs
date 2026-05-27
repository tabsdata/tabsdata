//
// Copyright 2026 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, SecAdmin, System};
use td_authz::{Authz, AuthzContext};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::group::Group;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListGroupService,
    request = ListRequest<()>,
    response = ListResponse<Group>,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        // List + read open to any authenticated role.
        from_fn(Authz::<SecAdmin, CollAdmin>::check),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<Group>, _>),
        from_fn(By::<()>::list::<NoFilter, Group>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{AccessTokenId, Description, GroupName, RoleId, UserId};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_list_groups_includes_seeds(db: TdPool) -> Result<(), TdError> {
        let _ = db
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
        .list((), ListParams::default());

        let service = ListGroupService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await?;
        // 3 seeded system groups + 1 user-created.
        assert_eq!(response.len, 4);
        let names: Vec<_> = response.data.iter().map(|g| g.name.clone()).collect();
        assert!(names.contains(&GroupName::default_group()));
        assert!(names.contains(&GroupName::source()));
        assert!(names.contains(&GroupName::destination()));
        assert!(names.contains(&GroupName::try_from("analytics")?));
        Ok(())
    }
}
