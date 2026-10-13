//
// Copyright 2025 Tabsdata Inc.
//

use td_database::domain::Apiserver;
use td_error::{TdError, td_error};
use td_objects::dxo::auth::SessionDBWithNames;
use td_objects::types::basic::{AccessTokenId, SessionStatus};
use td_queries::{Queries, QueryExecutor};

pub type Session = SessionDBWithNames;
pub type SessionMap = HashMap<AccessTokenId, Arc<Session>>;

#[td_error]
pub enum SessionError {
    #[error("Session not found: {0}")]
    NotFound(AccessTokenId) = 4000,
}

pub struct Sessions {
    cache: Mutex<Option<Arc<SessionMap>>>,
    last_purge: Mutex<DateTime<Utc>>,
}

impl Default for Sessions {
    fn default() -> Self {
        Self {
            cache: Mutex::new(None),
            last_purge: Mutex::new(DateTime::from_timestamp(0, 0).unwrap().to_utc()),
        }
    }
}

impl Sessions {
    const PURGE_INTERVAL_SECS: i64 = 60 * 60;

    pub async fn cached(&self) -> Option<Arc<SessionMap>> {
        self.cache.lock().await.clone()
    }

    pub async fn get(
        &self,
        conn: impl QueryExecutor<Domain = Apiserver>,
    ) -> Result<Arc<SessionMap>, TdError> {
        let mut cache = self.cache.lock().await;
        if let Some(map) = cache.as_ref() {
            return Ok(map.clone());
        }
        let sessions: Vec<Session> = conn.fetch_all(&SessionStatus::Active).await?;
        let map = Arc::new(
            sessions
                .into_iter()
                .map(|s| (s.base.access_token_id, Arc::new(s)))
                .collect::<SessionMap>(),
        );
        *cache = Some(map.clone());
        Ok(map)
    }

    pub async fn purge(&self, conn: impl Writes<Domain = Apiserver> + Send) -> Result<(), TdError> {
        if self.time_to_purge().await {
            conn.sessions().purge().await?;
        }
        Ok(())
    }

    /// Run after the transaction commits: invalidating inside it races a
    /// concurrent reader refilling the cache from a pre-commit snapshot.
    pub async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }

    async fn time_to_purge(&self) -> bool {
        let now = Utc::now();
        let mut last_purge = self.last_purge.lock().await;
        let purge =
            now.signed_duration_since(*last_purge).num_seconds() > Self::PURGE_INTERVAL_SECS;
        if purge {
            *last_purge = now;
        }
        purge
    }
}
