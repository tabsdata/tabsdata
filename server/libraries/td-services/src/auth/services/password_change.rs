//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use crate::auth::layers::assert_current_password::assert_current_password;
use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_password_hash::create_password_hash;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::session::Sessions;
use ta_services::factory::service_factory;
use td_error::TdError;
use td_objects::dxo::auth::defs::{
    PasswordChange, SessionDB, SessionPasswordChangeDB, SessionPasswordChangeDBBuilder,
};
use td_objects::dxo::user::defs::{UserDB, UserDBBuilder};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractService, SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::bool::PasswordMustChange;
use td_objects::types::id::UserId;
use td_objects::types::string::{NewPassword, OldPassword, PasswordHash, UserName};
use td_objects::types::timestamp::{AtTime, PasswordChangeTime};
use td_security::config::PasswordHashingConfig;
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;
use tower::util::MapErrLayer;

#[service_factory(
    name = PasswordChangeService,
    request = PasswordChange,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = PasswordHashingConfig,
    context = Sessions,
)]
fn service() {
    layers!(
        layers!(
            // return this type of error for this layer group
            MapErrLayer::new(|_err| TdError::from(AuthError::AuthenticationFailed)),
            // setting request time (we don't have request context in this service)
            from_fn(With::<AtTime>::default),
            // extract info from request
            from_fn(With::<PasswordChange>::extract::<UserName>),
            from_fn(With::<PasswordChange>::extract::<OldPassword>),
            from_fn(With::<PasswordChange>::extract::<NewPassword>),
            // get user from DB
            from_fn(By::<UserName>::select::<UserDB>),
            from_fn(With::<UserDB>::extract::<UserId>),
            // check current password
            from_fn(With::<UserDB>::extract::<PasswordHash>),
            from_fn(assert_current_password::<OldPassword>)
        ),
        layers!(
            // check user is enabled
            from_fn(assert_user_enabled),
            // create new password hash
            from_fn(create_password_hash::<NewPassword>),
            from_fn(With::<UserDB>::convert_to::<UserDBBuilder, _>),
            from_fn(With::<PasswordHash>::set::<UserDBBuilder>),
            // update password change time
            from_fn(With::<AtTime>::convert_to::<PasswordChangeTime, _>),
            from_fn(With::<PasswordChangeTime>::set::<UserDBBuilder>),
            // reset the password must change flag
            from_fn(With::<PasswordMustChange>::default),
            from_fn(With::<PasswordMustChange>::set::<UserDBBuilder>),
            // update user in DB
            from_fn(With::<UserDBBuilder>::build::<UserDB, _>),
            from_fn(By::<UserId>::update::<UserDB, UserDB>),
            // invalidate all existing user sessions for the user id
            from_fn(With::<UserDB>::extract::<UserId>),
            from_fn(With::<SessionPasswordChangeDBBuilder>::default),
            from_fn(With::<AtTime>::set::<SessionPasswordChangeDBBuilder>),
            from_fn(With::<SessionPasswordChangeDBBuilder>::build::<SessionPasswordChangeDB, _>),
            from_fn(By::<UserId>::update::<SessionPasswordChangeDB, SessionDB>),
            // invalidate sessions cache
            from_fn(refresh_sessions),
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::auth::AuthError;
    use crate::auth::jwt::decode_token;
    use crate::auth::services::AuthServices;
    use crate::auth::services::tests::get_session;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_error::assert_service_error;
    use td_objects::dxo::auth::defs::{Login, PasswordChange};
    use td_objects::types::string::{Password, RoleName};
    use td_objects::types::typed_enum::SessionStatus;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_password_change(db: DbPool) {
        use crate::auth::layers::assert_current_password::assert_current_password;
        use crate::auth::layers::assert_user_enabled::assert_user_enabled;
        use crate::auth::layers::create_password_hash::create_password_hash;
        use crate::auth::layers::refresh_sessions::refresh_sessions;
        use crate::auth::services::password_change::PasswordChangeService;
        use td_objects::dxo::auth::defs::{
            PasswordChange, SessionDB, SessionPasswordChangeDB, SessionPasswordChangeDBBuilder,
        };
        use td_objects::tower_service::from::{
            BuildService, DefaultService, ExtractService, SetService, TryIntoService, With,
        };
        use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
        use td_tower::metadata::type_of_val;

        PasswordChangeService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<PasswordChange, ()>(&[
                type_of_val(&With::<AtTime>::default),
                type_of_val(&With::<PasswordChange>::extract::<UserName>),
                type_of_val(&With::<PasswordChange>::extract::<OldPassword>),
                type_of_val(&With::<PasswordChange>::extract::<NewPassword>),
                type_of_val(&By::<UserName>::select::<UserDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&With::<UserDB>::extract::<PasswordHash>),
                type_of_val(&assert_current_password::<OldPassword>),
                type_of_val(&assert_user_enabled),
                type_of_val(&create_password_hash::<NewPassword>),
                type_of_val(&With::<UserDB>::convert_to::<UserDBBuilder, _>),
                type_of_val(&With::<PasswordHash>::set::<UserDBBuilder>),
                type_of_val(&With::<AtTime>::convert_to::<PasswordChangeTime, _>),
                type_of_val(&With::<PasswordChangeTime>::set::<UserDBBuilder>),
                type_of_val(&With::<PasswordMustChange>::default),
                type_of_val(&With::<PasswordMustChange>::set::<UserDBBuilder>),
                type_of_val(&With::<UserDBBuilder>::build::<UserDB, _>),
                type_of_val(&By::<UserId>::update::<UserDB, UserDB>),
                type_of_val(&With::<UserDB>::extract::<UserId>),
                type_of_val(&With::<SessionPasswordChangeDBBuilder>::default),
                type_of_val(&With::<AtTime>::set::<SessionPasswordChangeDBBuilder>),
                type_of_val(
                    &With::<SessionPasswordChangeDBBuilder>::build::<SessionPasswordChangeDB, _>,
                ),
                type_of_val(&By::<UserId>::update::<SessionPasswordChangeDB, SessionDB>),
                type_of_val(&refresh_sessions),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_password_change_ok(db: DbPool) -> Result<(), TdError> {
        let context = Context::with_defaults(db.clone());
        let auth_services = AuthServices::build(&context);

        // doing a login before password change to verify it will be invalidated
        let service = auth_services.login.service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id = *decode_token(&context.jwt_config, access_token)?.jti();

        let service = auth_services.password_change.service().await;

        let request = PasswordChange::builder()
            .name(UserName::try_from("admin")?)
            .old_password(OldPassword::try_from("tabsdata")?)
            .new_password(NewPassword::try_from("tabsdata1")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());

        let session = get_session(&db, &access_token_id.into()).await;
        match session {
            Some(session) => {
                assert_eq!(session.status, SessionStatus::InvalidPasswordChange);
            }
            None => {
                panic!("Session not found");
            }
        }

        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_password_change_wrong_user_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = AuthServices::build(&Context::with_defaults(db.clone()));
        let service = auth_services.password_change.service().await;

        let request = PasswordChange::builder()
            .name(UserName::try_from("invalid")?)
            .old_password(OldPassword::try_from("tabsdata")?)
            .new_password(NewPassword::try_from("tabsdata1")?)
            .build()?;
        assert_service_error(service, request, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        })
        .await;
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_password_change_wrong_old_password_unauthz(
        db: DbPool,
    ) -> Result<(), td_error::TdError> {
        let auth_services = AuthServices::build(&Context::with_defaults(db.clone()));
        let service = auth_services.password_change.service().await;

        let request = PasswordChange::builder()
            .name(UserName::try_from("admin")?)
            .old_password(OldPassword::try_from("invalidpassword")?)
            .new_password(NewPassword::try_from("tabsdata1")?)
            .build()?;
        assert_service_error(service, request, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        })
        .await;
        Ok(())
    }
}
