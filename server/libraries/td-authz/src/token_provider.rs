//
// Copyright 2026 Tabsdata Inc.
//

//! Service tokens keyed by digest: a whole-map snapshot with explicit
//! invalidation, like the session cache.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use td_common::provider::{CachedProvider, Provider};
use td_database::domain::{Apiserver, Domained};
use td_error::{TdError, td_error};
use td_objects::dxo::service_token::{ServiceTokenDBWithNames, ServiceTokenTouchDB};
use td_objects::types::basic::{AtTime, ServiceTokenId, TokenDigest};
use td_queries::{Queries, QueryExecutor, Writes};

type Tokens = HashMap<TokenDigest, Arc<ServiceTokenDBWithNames>>;

/// One `last_used_on` write per token per interval per node.
const TOUCH_INTERVAL_SECS: i64 = 10 * 60;

#[td_error]
pub enum TokenError {
    #[error("Service token expired")]
    Expired = 4001,
    #[error("The account owning this service token is disabled")]
    OwnerDisabled = 4002,
}

#[derive(Default)]
pub struct SqlTokenProvider;

impl<E: QueryExecutor + Domained<Domain = Apiserver> + Send> Provider<Tokens, E>
    for SqlTokenProvider
{
    async fn get(&self, conn: E) -> Result<Arc<Tokens>, TdError> {
        let tokens: Vec<ServiceTokenDBWithNames> = conn.fetch_all(&()).await?;
        let tokens = tokens
            .into_iter()
            .map(|token| (token.base.digest.clone(), Arc::new(token)))
            .collect();
        Ok(Arc::new(tokens))
    }

    /// An expired token keeps its slot until revoked.
    async fn purge(&self, _conn: E) -> Result<(), TdError> {
        Ok(())
    }
}

#[derive(Default)]
pub struct ServiceTokens {
    cache: CachedProvider<Tokens, SqlTokenProvider>,
    touched: Mutex<HashMap<ServiceTokenId, DateTime<Utc>>>,
}

impl ServiceTokens {
    pub async fn cached(&self) -> Option<Arc<Tokens>> {
        self.cache.cached().await
    }

    pub async fn load<E>(&self, conn: E) -> Result<Arc<Tokens>, TdError>
    where
        E: QueryExecutor + Domained<Domain = Apiserver> + Send,
    {
        self.cache.get(conn).await
    }

    /// Call after the transaction commits: a reload from a pre-commit snapshot
    /// would stick.
    pub async fn invalidate(&self) {
        self.cache.invalidate().await;
        self.touched.lock().unwrap().clear();
    }

    pub fn accept(&self, token: &ServiceTokenDBWithNames, at: &AtTime) -> Result<(), TdError> {
        if !token.base.expires_on.is_live_at(at) {
            Err(TokenError::Expired)?
        }
        if !*token.user_enabled {
            Err(TokenError::OwnerDisabled)?
        }
        Ok(())
    }

    /// Check before acquiring a write connection.
    pub fn due_for_touch(&self, id: &ServiceTokenId, now: DateTime<Utc>) -> bool {
        let mut touched = self.touched.lock().unwrap();
        let due = touched.get(id).is_none_or(|last| {
            now.signed_duration_since(*last).num_seconds() >= TOUCH_INTERVAL_SECS
        });
        if due {
            touched.insert(*id, now);
        }
        due
    }

    /// Call only when [`Self::due_for_touch`] said so.
    pub async fn touch(
        &self,
        conn: impl Writes<Domain = Apiserver> + Send,
        id: &ServiceTokenId,
        at: &AtTime,
    ) -> Result<(), TdError> {
        let touch = ServiceTokenTouchDB {
            last_used_on: Some((*at).into()),
        };
        conn.update(&touch, *id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ServiceTokenDBWithNames, ServiceTokens, TokenError};
    use chrono::{Duration, Utc};
    use td_common::datetime::Timestamp;
    use td_objects::dxo::service_token::ServiceTokenDB;
    use td_objects::types::basic::{
        AtTime, Created, ServiceTokenId, TokenDigest, TokenExpiresOn, TokenHint, TokenScope,
        TokenSlot, TokenSystem, UserEnabled, UserId, UserName,
    };

    fn token(expires_on: TokenExpiresOn, enabled: bool) -> ServiceTokenDBWithNames {
        ServiceTokenDBWithNames {
            base: ServiceTokenDB {
                id: ServiceTokenId::default(),
                user_id: UserId::admin(),
                slot: TokenSlot::try_from(1).unwrap(),
                digest: TokenDigest::try_from("0".repeat(64)).unwrap(),
                hint: TokenHint::try_from("00000000").unwrap(),
                comment: None,
                created_on: Created::from(AtTime::default()),
                expires_on,
                last_used_on: None,
                scope: TokenScope::Delegated,
                system: TokenSystem::FALSE,
            },
            user_name: UserName::admin(),
            user_enabled: UserEnabled::from(enabled),
        }
    }

    fn in_days(days: i64) -> TokenExpiresOn {
        TokenExpiresOn::from(Timestamp::from(Utc::now() + Duration::days(days)))
    }

    #[test]
    fn test_accept_refuses_expired_and_disabled() {
        let tokens = ServiceTokens::default();
        let now = AtTime::default();
        tokens.accept(&token(in_days(1), true), &now).unwrap();
        tokens
            .accept(&token(TokenExpiresOn::never(), true), &now)
            .unwrap();

        let err = tokens.accept(&token(in_days(-1), true), &now).unwrap_err();
        assert!(matches!(err.domain_err(), TokenError::Expired));

        let err = tokens.accept(&token(in_days(1), false), &now).unwrap_err();
        assert!(matches!(err.domain_err(), TokenError::OwnerDisabled));
    }

    #[test]
    fn test_touch_is_due_once_per_interval() {
        let tokens = ServiceTokens::default();
        let id = ServiceTokenId::default();
        let start = Utc::now();
        assert!(tokens.due_for_touch(&id, start));
        assert!(!tokens.due_for_touch(&id, start + Duration::minutes(9)));
        assert!(tokens.due_for_touch(&id, start + Duration::minutes(10)));
        assert!(tokens.due_for_touch(&ServiceTokenId::default(), start));
    }

    #[tokio::test]
    async fn test_invalidate_forgets_touches() {
        let tokens = ServiceTokens::default();
        let id = ServiceTokenId::default();
        let start = Utc::now();
        assert!(tokens.due_for_touch(&id, start));
        tokens.invalidate().await;
        assert!(tokens.due_for_touch(&id, start));
    }
}
