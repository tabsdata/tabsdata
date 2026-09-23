//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use crate::auth::assert_current_password::assert_current_password;
use crate::auth::assert_user_enabled::assert_user_enabled;
use argon2::PasswordHasher;
use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use std::ops::Deref;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::session_provider::Sessions;
use td_database::access::{AccessMode, DbPool, DbScope, Global, Grant};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::auth::{PasswordChange, SessionInvalidateDB};
use td_objects::dxo::user::{UserDB, UserUpdateDB};
use td_objects::types::basic::PasswordHash;
use td_objects::types::basic::{AtTime, PasswordChangeTime, PasswordMustChange, SessionStatus};
use td_queries::Queries;
use td_security::config::PasswordHashingConfig;

#[service_factory]
async fn password_change(
    password_hashing_config: Ctx<PasswordHashingConfig>,
    sessions: Ctx<Sessions>,
    db: DbPool<Global>,
    request: PasswordChange,
) -> Result<(), TdError> {
    let mut gated_trx = db.begin().await?;
    let conn = gated_trx.get(Grant::asserted("password-change credential verification"));
    // request time (we don't have a request context in this service)
    let at = AtTime::default();

    // Unknown user and wrong current password both surface as
    // AuthenticationFailed so neither can be distinguished.
    let user = authenticate(conn, &request)
        .await
        .map_err(|_| TdError::from(AuthError::AuthenticationFailed))?;
    // check user is enabled (keeps its own code so a valid login can act on it)
    assert_user_enabled(&user)?;

    let hash = create_password_hash(&password_hashing_config, &request.new_password)?;
    let updated = UserUpdateDB {
        full_name: user.full_name.clone(),
        email: Some(user.email.clone()),
        modified_on: at.into(),
        modified_by_id: user.id.into(),
        password_hash: hash,
        password_set_on: PasswordChangeTime::try_from(&at)?,
        password_must_change: PasswordMustChange::FALSE,
        enabled: user.enabled,
    };
    conn.update::<UserUpdateDB>(&updated, user.id).await?;

    // invalidate all existing sessions for the user id
    let invalidate = SessionInvalidateDB::new(at, SessionStatus::InvalidPasswordChange);
    conn.update(&invalidate, user.id).await?;

    // Housekeeping purge commits with the transaction; the cache invalidation
    // must wait for the commit (see LoginService).
    sessions.purge(conn).await?;
    gated_trx.commit().await?;
    sessions.invalidate().await;

    Ok(())
}

async fn authenticate<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    request: &PasswordChange,
) -> Result<UserDB, TdError> {
    let user: UserDB = conn.fetch_one(&request.name).await?;
    assert_current_password(&user.password_hash, &request.old_password)?;
    Ok(user)
}

pub fn create_password_hash<P: Deref<Target = String>>(
    password_hashing_config: &PasswordHashingConfig,
    password: &P,
) -> Result<PasswordHash, TdError> {
    let hash = password_hashing_config
        .password_hasher()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
        .unwrap()
        .to_string();

    PasswordHash::try_from(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::auth::AuthError;
    use td_common::jwt::decode_token;
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::dxo::auth::SessionDB;
    use td_objects::dxo::auth::{Login, PasswordChange};
    use td_objects::types::basic::AccessTokenId;
    use td_objects::types::basic::UserName;
    use td_objects::types::basic::{NewPassword, OldPassword};
    use td_objects::types::basic::{Password, SessionStatus};
    use td_queries::Queries;

    #[td_test::test(db)]
    async fn test_password_change_ok(db: TdPool) -> Result<(), TdError> {
        let context = Services::with_defaults(db.clone());
        let auth_services = context.clone();

        // doing a login before password change to verify it will be invalidated
        let service = auth_services.clone();

        let request = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        let res = service.login(request).await;
        assert!(res.is_ok());
        let token_response = res?;
        let access_token = &token_response.access_token;
        let access_token_id =
            AccessTokenId::from(decode_token(&context.jwt_config, access_token)?.jti);

        let service = auth_services.clone();

        let request = PasswordChange {
            name: UserName::try_from("admin")?,
            old_password: OldPassword::try_from("tabsdata")?,
            new_password: NewPassword::try_from("tabsdata1")?,
        };
        let res = service.password_change(request).await;
        assert!(res.is_ok());

        let session = db.fetch_optional::<SessionDB>(access_token_id).await?;
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

    #[td_test::test(db)]
    async fn test_password_change_wrong_user_unauthz(db: TdPool) -> Result<(), td_error::TdError> {
        let auth_services = Services::with_defaults(db.clone());
        let service = auth_services.clone();

        let request = PasswordChange {
            name: UserName::try_from("invalid")?,
            old_password: OldPassword::try_from("tabsdata")?,
            new_password: NewPassword::try_from("tabsdata1")?,
        };
        assert_service_error(service.password_change(request).await, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        });
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_password_change_wrong_old_password_unauthz(
        db: TdPool,
    ) -> Result<(), td_error::TdError> {
        let auth_services = Services::with_defaults(db.clone());
        let service = auth_services.clone();

        let request = PasswordChange {
            name: UserName::try_from("admin")?,
            old_password: OldPassword::try_from("invalidpassword")?,
            new_password: NewPassword::try_from("tabsdata1")?,
        };
        assert_service_error(service.password_change(request).await, |err| match err {
            AuthError::AuthenticationFailed => {}
            other => panic!("Expected 'AuthenticationFailed', got {other:?}"),
        });
        Ok(())
    }
}
