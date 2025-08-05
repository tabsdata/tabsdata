//
// Copyright 2024 Tabs Data Inc.
//

use crate::user::layers::delete::delete_user_validate;
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::UserParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectService};
use td_objects::types::basic::{UserId, UserIdName};
use td_objects::types::role::UserRoleDB;
use td_objects::types::user::UserDB;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = DeleteUserService,
    request = DeleteRequest<UserParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<DeleteRequest<UserParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SecAdmin>::check),
        from_fn(With::<DeleteRequest<UserParam>>::extract_name::<UserParam>),
        from_fn(With::<UserParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::select::<UserDB>),
        from_fn(With::<RequestContext>::extract::<UserId>),
        from_fn(delete_user_validate),
        from_fn(With::<UserDB>::extract::<UserId>),
        from_fn(By::<UserId>::delete::<UserRoleDB>),
        from_fn(By::<UserId>::delete::<UserDB>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::UserParam;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_user::seed_user;
    use td_objects::types::basic::{AccessTokenId, RoleId, UserEnabled, UserId, UserName};
    use td_objects::types::user::UserDB;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_provider(db: DbPool) {
        use td_tower::metadata::type_of_val;

        DeleteUserService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<DeleteRequest<UserParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<UserParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SecAdmin>::check),
                type_of_val(&With::<DeleteRequest<UserParam>>::extract_name::<UserParam>),
                type_of_val(&With::<UserParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::select::<UserDB>),
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(&delete_user_validate),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&By::<UserId>::delete::<UserRoleDB>),
                type_of_val(&By::<UserId>::delete::<UserDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_user(db: DbPool) {
        let user = seed_user(
            &db,
            &UserName::try_from("u0").unwrap(),
            &UserEnabled::from(true),
        )
        .await;

        let service = DeleteUserService::with_defaults(db.clone())
            .await
            .service()
            .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
        )
        .delete(
            UserParam::builder()
                .try_user("u0")
                .unwrap()
                .build()
                .unwrap(),
        );
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let res: Option<UserDB> = DaoQueries::default()
            .select_by::<UserDB>(&(&UserName::try_from("u0").unwrap()))
            .unwrap()
            .build_query_as()
            .fetch_optional(&db)
            .await
            .unwrap();
        assert!(res.is_none());

        let res: Option<UserRoleDB> = DaoQueries::default()
            .select_by::<UserRoleDB>(&user.id())
            .unwrap()
            .build_query_as()
            .fetch_optional(&db)
            .await
            .unwrap();
        assert!(res.is_none());
    }
}
