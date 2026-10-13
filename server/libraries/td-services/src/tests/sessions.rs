//
// Copyright 2026 Tabsdata Inc.
//

//! A user's sessions stop being valid the moment an admin takes their access
//! away.

use crate::Services;
use crate::auth::set_session_expiration::session_expiration;
use chrono::TimeDelta;
use std::str::FromStr;
use std::time::Duration;
use td_common::datetime::Timestamp;
use td_common::jwt::decode_token;
use td_database::access::Attempt;
use td_database::access::{DbPool, GatedConnection, Global, Grant, RW};
use td_database::connection::PgConnectionExt;
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::{ApiError, TdError};
use td_objects::dxo::auth::{Login, PasswordChange, SessionDB, SessionInvalidateDB};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::user::{UserDB, UserParam, UserUpdate};
use td_objects::types::basic::{
    AccessTokenId, AtTime, NewPassword, OldPassword, Password, SessionStatus, UserEnabled, UserId,
    UserIdName, UserName,
};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn admin() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

async fn logged_in_user(
    db: &TdPool,
    services: &Services,
    name: &str,
) -> Result<AccessTokenId, TdError> {
    db.seed()
        .user(&UserName::try_from(name)?, &UserEnabled::TRUE)
        .await?;
    let token = services
        .clone()
        .login(Login {
            name: UserName::try_from(name)?,
            password: Password::try_from("password")?,
        })
        .await?;
    Ok(AccessTokenId::from(
        decode_token(&services.jwt_config, &token.access_token)?.jti,
    ))
}

fn param(name: &str) -> Result<UserParam, TdError> {
    Ok(UserParam {
        user: UserIdName::from_str(name)?,
    })
}

async fn assert_session_ended(
    db: &TdPool,
    session: AccessTokenId,
    why: &str,
) -> Result<(), TdError> {
    let status = db
        .fetch_optional::<SessionDB>(session)
        .await?
        .map(|s| s.status);
    assert_ne!(
        status,
        Some(SessionStatus::Active),
        "{why} must end the user's sessions"
    );
    Ok(())
}

/// Disabling a user takes their access away; the sessions they
/// already hold must end with it.
#[td_test::test(db)]
async fn test_disabling_a_user_ends_their_sessions(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let session = logged_in_user(&db, &services, "u_disabled").await?;
    let update = UserUpdate {
        full_name: None,
        email: None,
        password: None,
        enabled: Some(UserEnabled::FALSE),
    };
    services
        .clone()
        .update_user(admin().update(param("u_disabled")?, update))
        .await?;
    assert_session_ended(&db, session, "disabling a user").await
}

/// An admin resetting a user's password.
#[td_test::test(db)]
async fn test_an_admin_password_reset_ends_the_users_sessions(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let session = logged_in_user(&db, &services, "u_reset").await?;
    let update = UserUpdate {
        full_name: None,
        email: None,
        password: Some(Password::try_from("reset-by-admin")?),
        enabled: None,
    };
    services
        .clone()
        .update_user(admin().update(param("u_reset")?, update))
        .await?;
    assert_session_ended(&db, session, "an admin password reset").await
}

/// Deleting a user.
#[td_test::test(db)]
async fn test_deleting_a_user_ends_their_sessions(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let session = logged_in_user(&db, &services, "u_deleted").await?;
    services
        .clone()
        .delete_user(admin().delete(param("u_deleted")?))
        .await?;
    assert_session_ended(&db, session, "deleting a user").await
}

async fn gate(db: &TdPool) -> Result<GatedConnection<RW>, TdError> {
    DbPool::<Global>::new(DomainPools::of_apiserver(db.clone()), Global)
        .begin(Attempt::unretried())
        .await
}

fn password_change(name: &str) -> Result<PasswordChange, TdError> {
    Ok(PasswordChange {
        name: UserName::try_from(name)?,
        old_password: OldPassword::try_from("password")?,
        new_password: NewPassword::try_from("changed-password")?,
    })
}

/// A login's or refresh's new session for `user`, inserted on `gate` and left
/// uncommitted.
async fn new_session_held_open(
    gate: &mut GatedConnection<RW>,
    services: &Services,
    user: UserId,
) -> Result<AccessTokenId, TdError> {
    let session = SessionDB::new(
        user,
        AtTime::default(),
        session_expiration(&services.jwt_config)?,
    );
    gate.get(Grant::asserted("new session held open"))
        .insert(&session)
        .await?;
    Ok(session.access_token_id)
}

/// The user changes their password while a refresh of one of their
/// sessions is in flight (held here after it locked and retired the old
/// session and inserted the new one). The password change waits for the old
/// session; once both commit, the session the refresh created must not be
/// Active either.
#[td_test::test(db)]
async fn test_a_password_change_ends_a_session_refreshed_concurrently(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    let old = logged_in_user(&db, &services, "u_refreshing").await?;

    let mut gate = gate(&db).await?;
    let conn = gate.get(Grant::asserted("refresh held open"));
    let user: UserDB = conn.fetch_one(&UserName::try_from("u_refreshing")?).await?;
    let session: SessionDB = conn.for_update().fetch_one(&old).await?;
    assert_eq!(session.user_id, user.id);
    conn.update(
        &SessionInvalidateDB::new(AtTime::default(), SessionStatus::InvalidNewToken),
        old,
    )
    .await?;
    let refreshed = new_session_held_open(&mut gate, &services, session.user_id).await?;

    let changing = tokio::spawn({
        let services = services.clone();
        async move {
            services
                .password_change(password_change("u_refreshing")?)
                .await
        }
    });
    assert!(
        gate.get(Grant::asserted("blocking probe"))
            .wait_until_blocking(Duration::from_secs(10))
            .await
            .expect("blocking probe"),
        "the password change never waited on the refresh"
    );
    gate.commit().await?;
    changing.await.expect("password change task")?;

    assert_session_ended(&db, refreshed, "a password change").await
}

/// The user logs in with their old password while they change it
/// (the login held here after it read the user and inserted its session). Once
/// both are done, the session created with the old password must not be
/// Active: either the password change ends it, or the login is failed to be
/// run again and never creates it.
#[td_test::test(db)]
async fn test_a_password_change_ends_a_session_logged_in_concurrently(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    logged_in_user(&db, &services, "u_logging_in").await?;

    let mut gate = gate(&db).await?;
    let user: UserDB = gate
        .get(Grant::asserted("login held open"))
        .fetch_one(&UserName::try_from("u_logging_in")?)
        .await?;
    let logged_in = new_session_held_open(&mut gate, &services, user.id).await?;

    services
        .clone()
        .password_change(password_change("u_logging_in")?)
        .await?;
    if let Err(conflict) = gate.commit().await {
        assert_eq!(
            conflict.api_error(),
            ApiError::Unavailable,
            "the login failed for another reason: {conflict}"
        );
        let session: Option<SessionDB> = db.fetch_optional(&logged_in).await?;
        assert!(session.is_none(), "the failed login left its session");
        return Ok(());
    }

    assert_session_ended(&db, logged_in, "a password change").await
}

/// U refreshes a session while U's password change is in flight (held here
/// after its revoking statement reached an expired session of U first).
/// Neither may fail with a deadlock; when the database fails the password
/// change to let the refresh through, it runs again, as its service would.
#[td_test::test(db)]
async fn test_a_purging_refresh_and_a_password_change_do_not_deadlock(
    db: TdPool,
) -> Result<(), TdError> {
    let setup = Services::with_defaults(db.clone());
    db.seed()
        .user(&UserName::try_from("u_purging")?, &UserEnabled::TRUE)
        .await?;
    let past = AtTime::from(Timestamp::from(**AtTime::now() - TimeDelta::hours(2)));
    let user: UserDB = db.fetch_one(&UserName::try_from("u_purging")?).await?;
    let token = setup
        .clone()
        .login(Login {
            name: UserName::try_from("u_purging")?,
            password: Password::try_from("password")?,
        })
        .await?;
    let expired = SessionDB::new(user.id, past, past);
    db.insert(&expired).await?;
    let session = AccessTokenId::from(decode_token(&setup.jwt_config, &token.access_token)?.jti);

    let mut gate = gate(&db).await?;
    gate.get(Grant::asserted("password change held open"))
        .update(
            &SessionInvalidateDB::new(AtTime::default(), SessionStatus::InvalidPasswordChange),
            expired.access_token_id,
        )
        .await?;

    let services = setup.clone();
    let mut refreshing = tokio::spawn(async move {
        services
            .refresh(RequestContext::with(session, user.id).update((), token.refresh_token.clone()))
            .await
    });
    let finished = tokio::select! {
        refreshed = &mut refreshing => Some(refreshed),
        _ = gate.get(Grant::asserted("blocking probe")).wait_until_blocking(Duration::from_secs(10)) => None,
    };
    let revoked = gate
        .get(Grant::asserted("password change held open"))
        .update(
            &SessionInvalidateDB::new(AtTime::default(), SessionStatus::InvalidPasswordChange),
            user.id,
        )
        .await;
    let revoked = match revoked {
        Ok(_) => gate.commit().await,
        Err(error) => {
            drop(gate);
            Err(error.into())
        }
    };
    let refreshed = match finished {
        Some(refreshed) => refreshed,
        None => refreshing.await,
    }
    .expect("refresh task");

    if let Err(conflict) = revoked {
        assert_eq!(
            conflict.api_error(),
            ApiError::Unavailable,
            "the password change failed: {conflict}"
        );
        setup
            .clone()
            .password_change(password_change("u_purging")?)
            .await?;
    }
    if let Err(error) = refreshed {
        assert_ne!(
            error.api_error(),
            ApiError::InternalError,
            "the refresh failed: {error:?}"
        );
    }
    Ok(())
}
