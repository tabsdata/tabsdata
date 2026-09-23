//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use crate::auth::assert_current_password::assert_current_password;
use crate::auth::assert_user_enabled::assert_user_enabled;
use crate::auth::create_access_token::create_access_token;
use crate::auth::set_session_expiration::session_expiration;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::session_provider::Sessions;
use td_common::jwt::JwtConfig;
use td_database::access::{DbPool, Global, Grant};
use td_error::TdError;
use td_objects::dxo::auth::{Login, SessionDB, TokenResponse};
use td_objects::dxo::user::UserDB;
use td_objects::types::basic::AtTime;
use td_queries::Queries;

#[service_factory]
async fn login(
    jwt_config: Ctx<JwtConfig>,
    sessions: Ctx<Sessions>,
    db: DbPool<Global>,
    request: Login,
) -> Result<TokenResponse, TdError> {
    let mut gated_trx = db.begin().await?;
    let conn = gated_trx.get(Grant::asserted("login credential verification"));
    // request time (we don't have a request context in this service)
    let at = AtTime::default();

    // unknown user and wrong password both surface as AuthenticationFailed so
    // neither can be distinguished; enabled/password-change errors below keep
    // their own codes so a valid login can act on them
    let user: Option<UserDB> = conn.fetch_optional(&request.name).await?;
    let user = assert_login_user(user)?;
    assert_current_password(&user.password_hash, &request.password)?;
    assert_user_enabled(&user)?;
    assert_no_password_change_required(&user)?;

    let session = SessionDB::new(user.id, at, session_expiration(&jwt_config)?);
    conn.insert(&session).await?;

    let token = create_access_token(&jwt_config, &session)?;

    // Housekeeping purge of expired session rows commits with this
    // transaction; the authoritative cache invalidation must wait for the
    // commit (invalidating earlier races a concurrent reader repopulating the
    // cache from a pre-commit snapshot that omits the new session).
    sessions.purge(conn).await?;
    gated_trx.commit().await?;
    sessions.invalidate().await;

    Ok(token)
}

pub fn assert_login_user(user: Option<UserDB>) -> Result<UserDB, TdError> {
    user.ok_or_else(|| AuthError::AuthenticationFailed.into())
}

pub fn assert_no_password_change_required(user: &UserDB) -> Result<(), TdError> {
    if *user.password_must_change {
        Err(AuthError::UserMustChangePassword)?
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::auth::AuthError;
    use crate::auth::services::tests::assert_session;
    use td_common::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::types::basic::Password;
    use td_objects::types::basic::UserName;

    #[td_test::test(db)]
    async fn test_login_ok(db: TdPool) -> Result<(), td_error::TdError> {
        let context = Services::with_defaults(db.clone());
        let auth_services = context.clone();
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        let res = service.login(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id = decode_token(&context.jwt_config, access_token)?.jti;

        assert_session(&db, &Some(access_token_id.into())).await;

        assert_eq!(
            *token_response.expires_in,
            context.jwt_config.access_token_expiration
        );
        assert!(!token_response.refresh_token.is_empty());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_login_wrong_user_unauthz(db: TdPool) -> Result<(), td_error::TdError> {
        let auth_services = Services::with_defaults(db.clone());
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("invalid")?,
            password: Password::try_from("tabsdata")?,
        };

        assert_service_error(service.login(request).await, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        });

        assert_session(&db, &None).await;
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_login_wrong_password_unauthz(db: TdPool) -> Result<(), td_error::TdError> {
        let auth_services = Services::with_defaults(db.clone());
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("invalidpassword")?,
        };

        assert_service_error(service.login(request).await, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        });
        Ok(())
    }
}
