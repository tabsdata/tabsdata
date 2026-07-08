//
// Copyright 2025. Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use td_common::provider::{CachedProvider, Provider};
use td_database::domain::{Apiserver, Domained};
use td_error::{TdError, td_error};
use td_objects::dxo::auth::SessionDBWithNames;
use td_objects::types::basic::{AccessTokenId, SessionStatus};
use td_queries::{LiveEntry, QueryExecutor, SessionsEntry};

pub type Session = SessionDBWithNames;
pub type Sessions = CachedProvider<HashMap<AccessTokenId, Arc<Session>>, SqlSessionProvider>;

#[td_error]
pub enum SessionError {
    #[error("Session not found: {0}")]
    NotFound(AccessTokenId) = 4000,
}

pub struct SqlSessionProvider {
    last_purge: Mutex<DateTime<Utc>>,
}

impl Default for SqlSessionProvider {
    fn default() -> Self {
        Self {
            last_purge: Mutex::new(DateTime::from_timestamp(0, 0).unwrap().to_utc()),
        }
    }
}

impl SqlSessionProvider {
    fn time_to_purge(&self) -> bool {
        const PURGE_INTERVAL_SECS: i64 = 60 * 60;

        let now = Utc::now();
        let mut last_purge = self.last_purge.lock().unwrap();
        let purge = now.signed_duration_since(*last_purge).num_seconds() > PURGE_INTERVAL_SECS;
        if purge {
            *last_purge = now;
        }
        purge
    }
}

impl<E: QueryExecutor + Domained<Domain = Apiserver> + Send>
    Provider<HashMap<AccessTokenId, Arc<Session>>, E> for SqlSessionProvider
{
    async fn get(&self, conn: E) -> Result<Arc<HashMap<AccessTokenId, Arc<Session>>>, TdError> {
        let sessions: Vec<Session> = conn.live().fetch_all(&SessionStatus::Active).await?;
        let sessions = sessions
            .into_iter()
            .map(|s| (s.access_token_id, Arc::new(s)))
            .collect();
        Ok(Arc::new(sessions))
    }

    async fn purge(&self, conn: E) -> Result<(), TdError> {
        if self.time_to_purge() {
            conn.sessions().purge().await?;
        }
        Ok(())
    }
}
