//
// Copyright 2026 Tabsdata Inc.
//

//! A login's password check costs the same whether the user exists or not,
//! and is not paid for with a pooled connection held in a transaction.

use crate::Services;
use crate::auth::assert_current_password::assert_current_password;
use sqlx::PgPool;
use std::time::{Duration, Instant};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::auth::Login;
use td_objects::dxo::user::UserDB;
use td_objects::types::basic::{Password, UserEnabled, UserName};
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;
use td_queries::test_utils::probes::{backend_pid, in_transaction};

fn login(name: &str, password: &str) -> Result<Login, TdError> {
    Ok(Login {
        name: UserName::try_from(name)?,
        password: Password::try_from(password)?,
    })
}

async fn failed_login_time(services: &Services, name: &str) -> Result<Duration, TdError> {
    let mut fastest = Duration::MAX;
    for _ in 0..3 {
        let started = Instant::now();
        let failed = services.clone().login(login(name, "wrong-password")?).await;
        assert!(failed.is_err(), "{name} logged in with a wrong password");
        fastest = fastest.min(started.elapsed());
    }
    Ok(fastest)
}

/// A login for a user that does not exist must take about as long as
/// one with a wrong password for a user that does, or its response time tells
/// which user names exist. Asserted loosely: at least a quarter as long.
#[td_test::test(db)]
async fn test_an_unknown_user_login_takes_as_long_as_a_wrong_password(
    db: TdPool,
) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    db.seed()
        .user(&UserName::try_from("u_known")?, &UserEnabled::TRUE)
        .await?;

    let wrong_password = failed_login_time(&services, "u_known").await?;
    let unknown_user = failed_login_time(&services, "u_unknown").await?;
    assert!(
        unknown_user * 4 >= wrong_password,
        "an unknown user fails in {unknown_user:?}, a wrong password in {wrong_password:?}"
    );
    Ok(())
}

/// A login must not check the password inside an open transaction,
/// holding a pooled connection for the length of the check. Observed from the
/// pool's other connection: the login's backend never stays in one
/// transaction for as long as half a password check.
#[td_test::test(db, flavor = "multi_thread", worker_threads = 2)]
async fn test_a_login_checks_the_password_outside_a_transaction(db: TdPool) -> Result<(), TdError> {
    let services = Services::with_defaults(db.clone());
    db.seed()
        .user(&UserName::try_from("u_hashing")?, &UserEnabled::TRUE)
        .await?;
    let user: UserDB = db.fetch_one(&UserName::try_from("u_hashing")?).await?;
    let hash = user.password_hash.clone();
    let started = Instant::now();
    tokio::task::spawn_blocking(move || {
        let _ = assert_current_password(&hash, &Password::try_from("wrong-password").unwrap());
    })
    .await
    .expect("password check");
    let check = started.elapsed();

    let pool: &PgPool = db.as_ref();
    let mut observer = pool.acquire().await.expect("observer connection");
    let login_pid: i32 = {
        let mut other = pool.acquire().await.expect("login's connection");
        backend_pid(&mut other).await.expect("pid")
    };

    let logging_in = tokio::spawn({
        let services = services.clone();
        async move { services.login(login("u_hashing", "password")?).await }
    });
    let mut longest = Duration::ZERO;
    let mut open_since: Option<Instant> = None;
    while !logging_in.is_finished() {
        let in_transaction = in_transaction(&mut observer, login_pid)
            .await
            .expect("observe the login's connection");
        open_since = match (in_transaction, open_since) {
            (true, None) => Some(Instant::now()),
            (true, since) => since,
            (false, _) => None,
        };
        if let Some(since) = open_since {
            longest = longest.max(since.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    logging_in.await.expect("login task")?;

    assert!(
        longest * 2 < check,
        "the login held a transaction open for {longest:?}; a password check takes {check:?}"
    );
    Ok(())
}
