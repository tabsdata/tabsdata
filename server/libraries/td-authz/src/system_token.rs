//
// Copyright 2026 Tabsdata Inc.
//

//! Server-managed service tokens: seeded at boot from configuration, never
//! created or revoked through the token API. The seed reconciles: an owner
//! holds exactly the configured token, or none when nothing valid is
//! configured, since no API can remove a system token.

use td_database::access::{DbScope, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::service_token::ServiceTokenDB;
use td_objects::types::basic::{
    AtTime, ServiceToken, TokenComment, TokenDigest, TokenExpiresOn, TokenScope, TokenSlot,
    TokenSystem, UserId,
};
use td_queries::Queries;
use td_security::token;
use tracing::{error, info};

const SYSTEM_TOKEN_SLOT: i32 = 1;

#[td_error]
pub enum SystemTokenError {
    #[error("Slot {1} of user [{0}] holds a token the server does not manage")]
    SlotTaken(UserId, TokenSlot) = 5000,
}

/// Never expires, whatever the deployment's token lifetime ceiling, and
/// carries every grant `owner` holds.
pub async fn seed_system_token<S: DbScope, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    owner: &UserId,
    configured: Option<&ServiceToken>,
    comment: &TokenComment,
) -> Result<(), TdError> {
    let wanted = configured.filter(|value| {
        let valid = token::parse(value).is_some();
        if !valid {
            error!(
                user_id = %owner,
                "The configured token is not a service token; its owner cannot authenticate"
            );
        }
        valid
    });
    let digest = wanted
        .map(|value| TokenDigest::try_from(token::digest(value)))
        .transpose()?;

    let held: Vec<ServiceTokenDB> = conn.fetch_all(and!(owner, &TokenSystem::TRUE)).await?;
    for row in held
        .iter()
        .filter(|row| Some(&row.digest) != digest.as_ref())
    {
        conn.delete::<ServiceTokenDB>(row.id).await?;
        info!(user_id = %owner, slot = %row.slot, token_id = %row.id, "system token removed");
    }

    let (Some(value), Some(digest)) = (wanted, digest) else {
        return Ok(());
    };
    if held.iter().any(|row| row.digest == digest) {
        return Ok(());
    }

    let slot = TokenSlot::try_from(SYSTEM_TOKEN_SLOT)?;
    if conn
        .fetch_optional::<ServiceTokenDB>(and!(owner, &slot))
        .await?
        .is_some()
    {
        Err(SystemTokenError::SlotTaken(*owner, slot))?
    }
    let row = ServiceTokenDB::new(
        *owner,
        slot,
        value,
        Some(comment.clone()),
        AtTime::default(),
        TokenExpiresOn::never(),
        TokenScope::Identity,
        TokenSystem::TRUE,
    )?;
    conn.insert(&row).await?;
    info!(user_id = %owner, slot = %row.slot, token_id = %row.id, "system token seeded");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SystemTokenError, seed_system_token};
    use td_database::access::{Global, RW};
    use td_database::domain::{Apiserver, DomainConnection};
    use td_database::pool::TdPool;
    use td_error::assert_service_error;
    use td_objects::and;
    use td_objects::dxo::service_token::ServiceTokenDB;
    use td_objects::types::basic::{
        AtTime, Created, ServiceToken, TokenComment, TokenExpiresOn, TokenScope, TokenSlot,
        TokenSystem, UserId,
    };
    use td_queries::Queries;
    use td_security::token;

    fn fresh() -> ServiceToken {
        ServiceToken::try_from(token::generate()).unwrap()
    }

    fn comment() -> TokenComment {
        TokenComment::try_from("seeded").unwrap()
    }

    async fn held(db: &TdPool, owner: &UserId) -> Vec<ServiceTokenDB> {
        let mut conn = DomainConnection::<Apiserver, Global, RW>::new(db.begin().await.unwrap());
        conn.fetch_all(and!(owner, &TokenSystem::TRUE))
            .await
            .unwrap()
    }

    async fn seed(db: &TdPool, configured: Option<&ServiceToken>) {
        let mut conn = DomainConnection::<Apiserver, Global, RW>::new(db.begin().await.unwrap());
        seed_system_token(&mut conn, &UserId::admin(), configured, &comment())
            .await
            .unwrap();
        conn.commit().await.unwrap();
    }

    #[td_test::test(db)]
    async fn test_seed_inserts_an_identity_token_that_never_expires(db: TdPool) {
        let configured = fresh();
        let before = Created::from(AtTime::default());
        seed(&db, Some(&configured)).await;
        let after = Created::from(AtTime::default());

        let rows = held(&db, &UserId::admin()).await;
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.user_id, UserId::admin());
        assert_eq!(*row.slot, 1);
        assert_eq!(row.digest.as_str(), token::digest(&configured));
        assert_eq!(row.hint.as_str(), token::hint(&configured).unwrap());
        assert_eq!(row.comment, Some(comment()));
        assert!(before <= row.created_on && row.created_on <= after);
        assert_eq!(row.expires_on, TokenExpiresOn::never());
        assert!(row.last_used_on.is_none());
        assert_eq!(row.scope, TokenScope::Identity);
        assert_eq!(row.system, TokenSystem::TRUE);
    }

    #[td_test::test(db)]
    async fn test_reseeding_the_same_token_keeps_the_row(db: TdPool) {
        let configured = fresh();
        seed(&db, Some(&configured)).await;
        let first = held(&db, &UserId::admin()).await;
        seed(&db, Some(&configured)).await;
        let second = held(&db, &UserId::admin()).await;
        assert_eq!(first[0].id, second[0].id);
    }

    #[td_test::test(db)]
    async fn test_a_new_token_replaces_the_old_one(db: TdPool) {
        seed(&db, Some(&fresh())).await;
        let rotated = fresh();
        seed(&db, Some(&rotated)).await;

        let rows = held(&db, &UserId::admin()).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].digest.as_str(), token::digest(&rotated));
    }

    #[td_test::test(db)]
    async fn test_no_configured_token_removes_the_seeded_one(db: TdPool) {
        seed(&db, Some(&fresh())).await;
        seed(&db, None).await;
        assert!(held(&db, &UserId::admin()).await.is_empty());
    }

    #[td_test::test(db)]
    async fn test_a_malformed_token_is_treated_as_absent(db: TdPool) {
        seed(&db, Some(&fresh())).await;
        let legacy = ServiceToken::try_from("eyJhbGciOiJIUzI1NiJ9.e30.sig").unwrap();
        seed(&db, Some(&legacy)).await;
        assert!(held(&db, &UserId::admin()).await.is_empty());
    }

    #[td_test::test(db)]
    async fn test_seed_refuses_a_slot_held_by_a_delegated_token(db: TdPool) {
        let mut conn = DomainConnection::<Apiserver, Global, RW>::new(db.begin().await.unwrap());
        conn.insert(
            &ServiceTokenDB::new(
                UserId::admin(),
                TokenSlot::try_from(1).unwrap(),
                &fresh(),
                None,
                AtTime::default(),
                TokenExpiresOn::never(),
                TokenScope::Delegated,
                TokenSystem::FALSE,
            )
            .unwrap(),
        )
        .await
        .unwrap();

        assert_service_error(
            seed_system_token(&mut conn, &UserId::admin(), Some(&fresh()), &comment()).await,
            |err| match err {
                SystemTokenError::SlotTaken(_, _) => {}
            },
        );
    }
}
