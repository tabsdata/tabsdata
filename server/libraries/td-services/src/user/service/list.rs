//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService};
use td_objects::types::user::UserRead;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ListUsersService,
    request = ListRequest<()>,
    response = ListResponse<UserRead>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ListRequest<()>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(By::<()>::list::<(), NoListFilter, UserRead>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_list_provider(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ListUsersService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ListRequest<()>, ListResponse<UserRead>>(&[
                type_of_val(&With::<ListRequest<()>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&By::<()>::list::<(), NoListFilter, UserRead>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_list_users(db: DbPool) {
        let _ = seed_user(
            &db,
            &UserName::try_from("u0").unwrap(),
            &UserEnabled::from(true),
        )
        .await;
        let user1 = seed_user(
            &db,
            &UserName::try_from("u1").unwrap(),
            &UserEnabled::from(true),
        )
        .await;

        let service = ListUsersService::with_defaults(db.clone()).service().await;

        let request =
            RequestContext::with(AccessTokenId::default(), user1.id(), RoleId::sec_admin())
                .list((), ListParams::default());
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let list = response.unwrap();
        assert_eq!(*list.len(), 3);
        let users = list
            .data()
            .iter()
            .map(|u| u.name().clone())
            .collect::<HashSet<_>>();
        let expected = HashSet::from([
            UserName::admin(),
            UserName::try_from("u0").unwrap(),
            UserName::try_from("u1").unwrap(),
        ]);
        assert_eq!(expected, users);
    }
}
