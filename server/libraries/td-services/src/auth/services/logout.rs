//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::refresh_sessions::refresh_sessions;
use ta_services::factory::service_factory;
use td_authz::session_provider::Sessions;
use td_objects::dxo::auth::{SessionDB, SessionLogoutDB, SessionLogoutDBBuilder};
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AccessTokenId, AtTime};
use td_queries::By;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = LogoutService,
    request = UpdateRequest<(), ()>,
    response = (),
    connection = TransactionProvider,
    scope = "Global",
    context = Sessions,
)]
fn service() {
    layers!(
        // extract access token id and request time from request context
        from_fn(With::<UpdateRequest<(), ()>>::extract::<RequestContext>),
        from_fn(With::<RequestContext>::extract::<AccessTokenId>),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        // logout corresponding session
        from_fn(With::<SessionLogoutDBBuilder>::default),
        from_fn(With::<AtTime>::set::<SessionLogoutDBBuilder>),
        from_fn(With::<SessionLogoutDBBuilder>::build::<SessionLogoutDB>),
        from_fn(By::<AccessTokenId>::update::<SessionLogoutDB, SessionDB>),
        // invalidate sessions cache
        from_fn(refresh_sessions),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::auth::services::AuthServices;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_authz::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::auth::Login;
    use td_objects::types::basic::{Password, RoleId, RoleName, SessionStatus, UserId, UserName};
    use td_queries::LiveEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_logout(db: TdPool) {
        use td_tower::metadata::type_of_val;

        LogoutService::with_defaults(db.clone())
            .metadata()
            .await
            .assert_service::<UpdateRequest<(), ()>, ()>(&[
                // extract access token id and request time from request context
                type_of_val(&With::<UpdateRequest<(), ()>>::extract::<RequestContext>),
                type_of_val(&With::<RequestContext>::extract::<AccessTokenId>),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                // logout corresponding session
                type_of_val(&With::<SessionLogoutDBBuilder>::default),
                type_of_val(&With::<AtTime>::set::<SessionLogoutDBBuilder>),
                type_of_val(&With::<SessionLogoutDBBuilder>::build::<SessionLogoutDB>),
                type_of_val(&By::<AccessTokenId>::update::<SessionLogoutDB, SessionDB>),
                // invalidate sessions cache
                type_of_val(&refresh_sessions),
            ]);
    }

    #[td_test::test(db)]
    async fn test_logout_ok(db: TdPool) -> Result<(), TdError> {
        let context = Context::with_defaults(db.clone());
        let auth_services = AuthServices::build(&context);
        let service = auth_services.login.service();

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id =
            AccessTokenId::from(decode_token(&context.jwt_config, access_token)?.jti);

        let service = auth_services.logout.service();

        let request =
            RequestContext::with(access_token_id, UserId::admin(), RoleId::user()).update((), ());
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let session = db
            .live()
            .fetch_optional::<SessionDB>(access_token_id)
            .await?;
        match session {
            Some(session) => {
                assert_eq!(session.status, SessionStatus::InvalidLogout);
            }
            None => {
                panic!("Session not found");
            }
        }
        Ok(())
    }
}
