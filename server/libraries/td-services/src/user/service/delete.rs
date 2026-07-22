//
// Copyright 2024 Tabsdata Inc.
//

use crate::user::layers::delete::delete_user_validate;
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageAccess, Rbac, Scope, System};
use td_objects::dxo::crudl::{DeleteRequest, RequestContext};
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::rest_urls::UserParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{UserId, UserIdName};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = DeleteUserService,
    request = DeleteRequest<UserParam>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<DeleteRequest<UserParam>>::extract::<RequestContext>),
        from_fn(Scope::<System>::system),
        from_fn(Rbac::<System, ManageAccess>::check),
        from_fn(With::<DeleteRequest<UserParam>>::extract_name::<UserParam>),
        from_fn(With::<UserParam>::extract::<UserIdName>),
        from_fn(By::<UserIdName>::fetch_one::<UserDB>),
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
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::user::UserDB;
    use td_objects::rest_urls::UserParam;
    use td_objects::types::basic::{AccessTokenId, UserEnabled, UserName};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_delete_user(db: TdPool) {
        use td_tower::metadata::type_of_val;

        DeleteUserService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<DeleteRequest<UserParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<UserParam>>::extract::<RequestContext>),
                type_of_val(&Scope::<System>::system),
                type_of_val(&Rbac::<System, ManageAccess>::check),
                type_of_val(&With::<DeleteRequest<UserParam>>::extract_name::<UserParam>),
                type_of_val(&With::<UserParam>::extract::<UserIdName>),
                type_of_val(&By::<UserIdName>::fetch_one::<UserDB>),
                type_of_val(&With::<RequestContext>::extract::<UserId>),
                type_of_val(&delete_user_validate),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&By::<UserId>::delete::<UserRoleDB>),
                type_of_val(&By::<UserId>::delete::<UserDB>),
            ]);
    }

    #[td_test::test(db)]
    async fn test_delete_user(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::from(true))
            .await
            .unwrap();

        let service = DeleteUserService::with_defaults(db.clone()).service();

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).delete(
            UserParam::builder()
                .user(UserIdName::from_str("u0").unwrap())
                .build()
                .unwrap(),
        );
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let res: Option<UserDB> = db
            .live()
            .fetch_optional::<UserDB>(&UserName::try_from("u0").unwrap())
            .await
            .unwrap();
        assert!(res.is_none());

        let res: Option<UserRoleDB> = db
            .live()
            .fetch_optional::<UserRoleDB>(&user.id)
            .await
            .unwrap();
        assert!(res.is_none());
    }
}
