//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{DbPool, Global, Grant, retrying};
use td_queries::Queries;
use tracing::{trace, warn};

/// Deletes a batch of expired sessions in a unit of its own, run after the
/// caller's unit has committed: inside it, or on a second connection while
/// it is open, the purge would wait on rows the caller holds. A session
/// another request holds is passed over and left to a later purge. Failing
/// leaves the caller's committed write standing, so it is only logged.
pub async fn purge_expired_sessions(db: &DbPool<Global>) {
    let purged = retrying(|attempt| async {
        let mut gated_trx = db.begin(attempt).await?;
        let conn = gated_trx.get(Grant::asserted("expired sessions purge"));
        let deleted = conn.sessions().purge().await?;
        gated_trx.commit().await?;
        Ok(deleted)
    })
    .await;
    match purged {
        Ok(purged) => trace!(purged, "Expired sessions purged"),
        Err(e) => warn!("Error purging expired sessions: {e:#?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::purge_expired_sessions;
    use crate::Services;
    use chrono::TimeDelta;
    use td_common::datetime::Timestamp;
    use td_database::access::Attempt;
    use td_database::access::{DbPool, Global, Grant};
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::auth::{Login, SessionDB};
    use td_objects::types::basic::{AccessTokenId, AtTime, Password, UserEnabled, UserName};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    fn hours_from_now(hours: i64) -> AtTime {
        AtTime::from(Timestamp::from(**AtTime::now() + TimeDelta::hours(hours)))
    }

    async fn seed_session(
        db: &TdPool,
        user: &str,
        expires_on: AtTime,
    ) -> Result<SessionDB, TdError> {
        let user = db
            .seed()
            .user(&UserName::try_from(user)?, &UserEnabled::TRUE)
            .await?;
        let session = SessionDB::new(user.id, expires_on, expires_on);
        db.insert(&session).await?;
        Ok(session)
    }

    async fn session_ids(db: &TdPool) -> Result<Vec<AccessTokenId>, TdError> {
        let sessions: Vec<SessionDB> = db.fetch_all(()).await?;
        let mut ids: Vec<_> = sessions.into_iter().map(|s| s.access_token_id).collect();
        ids.sort();
        Ok(ids)
    }

    #[td_test::test(db)]
    async fn test_purge_passes_over_held_sessions(db: TdPool) -> Result<(), TdError> {
        let services = Services::with_defaults(db.clone());
        let pool = DbPool::<Global>::new(services.pools.clone(), Global);
        let held = seed_session(&db, "u_held", hours_from_now(-2)).await?;
        seed_session(&db, "u_free", hours_from_now(-2)).await?;
        let live = seed_session(&db, "u_live", hours_from_now(1)).await?;

        let mut request = pool.begin(Attempt::unretried()).await?;
        let _: SessionDB = request
            .get(Grant::asserted("request holding a session"))
            .for_update()
            .fetch_one(&held.access_token_id)
            .await?;

        purge_expired_sessions(&pool).await;
        request.commit().await?;

        let mut expected = vec![held.access_token_id, live.access_token_id];
        expected.sort();
        assert_eq!(session_ids(&db).await?, expected);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_login_purges_expired_sessions(db: TdPool) -> Result<(), TdError> {
        let services = Services::with_defaults(db.clone());
        seed_session(&db, "u_expired", hours_from_now(-2)).await?;
        let live = seed_session(&db, "u_live", hours_from_now(1)).await?;

        let login = Login {
            name: UserName::try_from("admin")?,
            password: Password::try_from("tabsdata")?,
        };
        services.login(login).await?;

        let sessions = session_ids(&db).await?;
        assert_eq!(sessions.len(), 2);
        assert!(sessions.contains(&live.access_token_id));
        Ok(())
    }
}
