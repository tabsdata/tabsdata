//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, Rbac, Scope, System};
use td_objects::NoFilter;
use td_objects::dxo::crudl::{ListParams, ListRequest, ListResponse, RequestContext};
use td_objects::dxo::user::UserRead;
use td_objects::list::params::ListQueryParams;
use td_objects::tower_service::with::With;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListUsersService,
    request = ListRequest<()>,
    response = ListResponse<UserRead>,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, ManageAccess>::check),
        from_fn(With::<ListRequest<()>>::extract::<ListParams>),
        from_fn(With::<ListParams>::try_convert_to::<ListQueryParams<UserRead>, _>),
        from_fn(By::<()>::list::<NoFilter, UserRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::types::basic::{AccessTokenId, UserEnabled, UserId, UserName};
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_list_users(db: TdPool) {
        use td_tower::metadata::type_of_val;

        ListUsersService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<UserRead>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, ManageAccess>::check),
                type_of_val(&With::<ListRequest<()>>::extract::<ListParams>),
                type_of_val(&With::<ListParams>::try_convert_to::<ListQueryParams<UserRead>, _>),
                type_of_val(&By::<()>::list::<NoFilter, UserRead>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_list_users(db: TdPool) {
        let _ = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
            .await;
        let _ = db
            .seed()
            .user(&UserName::try_from("u1").unwrap(), &UserEnabled::from(true))
            .await
            .unwrap();

        let service = ListUsersService::with_defaults(db.clone()).service();

        // `admin` is the only seeded user in `sys_admin` (holding manage_access),
        // so it is the requester able to list users.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .list((), ListParams::default());
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(list.len, 4);
        let system = list
            .data
            .iter()
            .find(|u| u.name == UserName::system())
            .expect("system user should be present");
        assert_eq!(system.enabled, UserEnabled::from(false));
        let users = list
            .data
            .into_iter()
            .map(|u| u.name)
            .collect::<HashSet<_>>();
        let expected = HashSet::from([
            UserName::system(),
            UserName::admin(),
            UserName::try_from("u0").unwrap(),
            UserName::try_from("u1").unwrap(),
        ]);
        assert_eq!(expected, users);
    }
}
