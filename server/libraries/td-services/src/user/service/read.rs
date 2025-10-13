//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::UserParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, Requester, SecAdmin, SystemOrUserId};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{UserId, UserIdName};
use td_objects::types::user::{UserDBWithNames, UserRead, UserReadBuilder};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = ReadUserService,
    request = ReadRequest<UserParam>,
    response = UserRead,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<UserParam>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<UserParam>>::extract_name::<UserParam>),
        from_fn(With::<UserParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::select::<UserDBWithNames>),
        from_fn(With::<UserDBWithNames>::extract::<UserId>),
        from_fn(AuthzOn::<SystemOrUserId>::set),
        from_fn(Authz::<SecAdmin, Requester>::check),
        from_fn(With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
        from_fn(With::<UserReadBuilder>::build::<UserRead, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::UserParam;
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_provider(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ReadUserService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<UserParam>, UserRead>(&[
                type_of_val(&With::<ReadRequest<UserParam>>::extract::<RequestContext>),
                type_of_val(&With::<ReadRequest<UserParam>>::extract_name::<UserParam>),
                type_of_val(&With::<UserParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::select::<UserDBWithNames>),
                type_of_val(&With::<UserDBWithNames>::extract::<UserId>),
                type_of_val(&AuthzOn::<SystemOrUserId>::set),
                type_of_val(&Authz::<SecAdmin, Requester>::check),
                type_of_val(&With::<UserDBWithNames>::convert_to::<UserReadBuilder, _>),
                type_of_val(&With::<UserReadBuilder>::build::<UserRead, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_user(db: DbPool) {
        let user_name = UserName::try_from("u0").unwrap();
        let user = seed_user(&db, &user_name, &UserEnabled::from(true)).await;

        let service = ReadUserService::with_defaults(db.clone()).service().await;

        let request = RequestContext::with(AccessTokenId::default(), user.id(), RoleId::user())
            .read(
                UserParam::builder()
                    .try_user(user_name.to_string())
                    .unwrap()
                    .build()
                    .unwrap(),
            );
        let response = service.raw_oneshot(request).await;
        // assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(created.id(), user.id());
        assert_eq!(*created.name(), user_name);
    }
}
