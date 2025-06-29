//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::layers::assert_current_password::assert_current_password;
use crate::auth::layers::assert_user_enabled::assert_user_enabled;
use crate::auth::layers::create_password_hash::create_password_hash;
use crate::auth::layers::refresh_sessions::refresh_sessions;
use crate::auth::services::PasswordHashConfig;
use crate::auth::session::Sessions;
use crate::auth::AuthError;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::from::{
    BuildService, DefaultService, ExtractService, SetService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::auth::{
    PasswordChange, SessionDB, SessionPasswordChangeDB, SessionPasswordChangeDBBuilder,
};
use td_objects::types::basic::{
    AtTime, NewPassword, OldPassword, PasswordChangeTime, PasswordHash, PasswordMustChange, UserId,
    UserName,
};
use td_objects::types::user::{UserDB, UserDBBuilder};
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::service_provider::{ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct PasswordChangeService {
    provider: ServiceProvider<PasswordChange, (), TdError>,
}

impl PasswordChangeService {
    pub fn new(
        db: DbPool,
        queries: Arc<DaoQueries>,
        password_hash_config: Arc<PasswordHashConfig>,
        sessions: Arc<Sessions<'static>>,
    ) -> Self {
        Self {
            provider: Self::provider(db, queries, password_hash_config, sessions),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, password_hash_config: Arc<PasswordHashConfig>, sessions: Arc<Sessions<'static>>) {
            service_provider!(
                layers!(
                    TransactionProvider::new(db),
                    SrvCtxProvider::new(queries),
                    SrvCtxProvider::new(password_hash_config),
                    SrvCtxProvider::new(sessions),
                ),
                layers!(
                    // setting request time (we don't have request context in this service)
                    from_fn(With::<AtTime>::default),

                    // extract info from request
                    from_fn(With::<PasswordChange>::extract::<UserName>),
                    from_fn(With::<PasswordChange>::extract::<OldPassword>),
                    from_fn(With::<PasswordChange>::extract::<NewPassword>),

                    // get user from DB
                    from_fn(By::<UserName>::select::<DaoQueries, UserDB>),
                    from_fn(With::<UserDB>::extract::<UserId>),

                    // check current password
                    from_fn(With::<UserDB>::extract::<PasswordHash>),
                    from_fn(assert_current_password::<OldPassword>);

                    // return this type of error for this layer group
                    map_err = |_err| TdError::from(AuthError::AuthenticationFailed)
                ),
                layers!(

                // check user is enabled
                    from_fn(assert_user_enabled),

                    // create new password hash
                    from_fn(create_password_hash::<NewPassword>),
                    from_fn(With::<UserDB>::convert_to::<UserDBBuilder,_>),
                    from_fn(With::<PasswordHash>::set::<UserDBBuilder>),

                    // update password change time
                    from_fn(With::<AtTime>::convert_to::<PasswordChangeTime, _>),
                    from_fn(With::<PasswordChangeTime>::set::<UserDBBuilder>),

                    // reset the password must change flag
                    from_fn(With::<PasswordMustChange>::default),
                    from_fn(With::<PasswordMustChange>::set::<UserDBBuilder>),

                    // update user in DB
                    from_fn(With::<UserDBBuilder>::build::<UserDB, _>),
                    from_fn(By::<UserId>::update::<DaoQueries, UserDB, UserDB>),

                    // invalidate all existing user sessions for the user id
                    from_fn(With::<UserDB>::extract::<UserId>),
                    from_fn(With::<SessionPasswordChangeDBBuilder>::default),
                    from_fn(With::<AtTime>::set::<SessionPasswordChangeDBBuilder>),
                    from_fn(With::<SessionPasswordChangeDBBuilder>::build::<SessionPasswordChangeDB, _>),
                    from_fn(By::<UserId>::update::<DaoQueries, SessionPasswordChangeDB, SessionDB>),

                    // invalidate sessions cache
                    from_fn(refresh_sessions),
                )
            )
        }
    }

    pub async fn service(&self) -> TdBoxService<PasswordChange, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::services::tests::{auth_services, get_session};
    use crate::auth::{decode_token, AuthError};
    use td_database::sql::DbPool;
    use td_error::assert_service_error;
    use td_objects::types::auth::{Login, PasswordChange};
    use td_objects::types::basic::{
        NewPassword, OldPassword, Password, RoleName, SessionStatus, UserName,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_password_change() {
        use crate::auth::layers::assert_current_password::assert_current_password;
        use crate::auth::layers::assert_user_enabled::assert_user_enabled;
        use crate::auth::layers::create_password_hash::create_password_hash;
        use crate::auth::layers::refresh_sessions::refresh_sessions;
        use crate::auth::services::password_change::PasswordChangeService;
        use crate::auth::services::PasswordHashConfig;
        use crate::auth::session;
        use std::sync::Arc;
        use td_objects::sql::DaoQueries;
        use td_objects::tower_service::from::{
            BuildService, DefaultService, ExtractService, SetService, TryIntoService, With,
        };
        use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
        use td_objects::types::auth::{
            PasswordChange, SessionDB, SessionPasswordChangeDB, SessionPasswordChangeDBBuilder,
        };
        use td_objects::types::basic::{
            AtTime, NewPassword, OldPassword, PasswordChangeTime, PasswordHash, PasswordMustChange,
            UserId, UserName,
        };
        use td_objects::types::user::{UserDB, UserDBBuilder};
        use td_tower::ctx_service::RawOneshot;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let service = PasswordChangeService::provider(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(PasswordHashConfig::default()),
            Arc::new(session::new(db.clone())),
        )
        .make()
        .await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();
        metadata.assert_service::<PasswordChange, ()>(&[
            type_of_val(&With::<AtTime>::default),
            type_of_val(&With::<PasswordChange>::extract::<UserName>),
            type_of_val(&With::<PasswordChange>::extract::<OldPassword>),
            type_of_val(&With::<PasswordChange>::extract::<NewPassword>),
            type_of_val(&By::<UserName>::select::<DaoQueries, UserDB>),
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
            type_of_val(&By::<UserId>::update::<DaoQueries, UserDB, UserDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&With::<SessionPasswordChangeDBBuilder>::default),
            type_of_val(&With::<AtTime>::set::<SessionPasswordChangeDBBuilder>),
            type_of_val(
                &With::<SessionPasswordChangeDBBuilder>::build::<SessionPasswordChangeDB, _>,
            ),
            type_of_val(&By::<UserId>::update::<DaoQueries, SessionPasswordChangeDB, SessionDB>),
            type_of_val(&refresh_sessions),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_password_change_ok(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        // doing a login before password change to verify it will be invalidated
        let service = auth_services.login_service().await;

        let request = Login::builder()
            .name(UserName::try_from("admin")?)
            .password(Password::try_from("tabsdata")?)
            .role(RoleName::try_from("user")?)
            .build()?;
        let res = service.raw_oneshot(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = token_response.access_token();
        let access_token_id = decode_token(auth_services.jwt_settings(), access_token)?.jti;

        let service = auth_services.password_change_service().await;

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
                assert_eq!(session.status(), &SessionStatus::InvalidPasswordChange);
            }
            None => {
                panic!("Session not found");
            }
        }

        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_password_change_wrong_user_unauthz(db: DbPool) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        let service = auth_services.password_change_service().await;

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
    async fn test_password_change_wrong_old_password_unauthz(
        db: DbPool,
    ) -> Result<(), td_error::TdError> {
        let auth_services = auth_services(&db).await;

        let service = auth_services.password_change_service().await;

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
