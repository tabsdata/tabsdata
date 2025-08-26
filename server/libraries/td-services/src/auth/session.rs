//
// Copyright 2025. Tabs Data Inc.
//

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use td_common::provider::{CachedProvider, Provider};
use td_error::{TdError, td_error};
use td_objects::crudl::{handle_delete_error, handle_select_error};
use td_objects::sql::{DaoQueries, DeleteBy, SelectBy};
use td_objects::types::auth::{SessionDB, SessionDBWithNames};
use td_objects::types::basic::{AccessTokenId, AtTime, SessionStatus};
use tracing::debug;

pub type Session = SessionDBWithNames;

#[td_error]
pub enum SessionError {
    #[error("Session not found: {0}")]
    NotFound(AccessTokenId) = 4000,

    #[error("Session not found: {0}")]
    CouldNotGetDbConn(sqlx::Error) = 5000,
}

#[async_trait]
pub trait SessionProvider<'a>:
    Provider<'a, HashMap<AccessTokenId, Arc<Session>>, &'a mut SqliteConnection>
{
    async fn get_session(
        &'a self,
        conn: &'a mut SqliteConnection,
        access_token_id: &AccessTokenId,
    ) -> Result<Arc<Session>, TdError> {
        Provider::get(self, conn)
            .await?
            .get(access_token_id)
            .cloned()
            .ok_or_else(|| SessionError::NotFound(*access_token_id).into())
    }
}

pub struct SqlSessionProvider {
    queries: DaoQueries,
    last_purge: Mutex<DateTime<Utc>>,
}

impl Default for SqlSessionProvider {
    fn default() -> Self {
        Self {
            queries: DaoQueries::default(),
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

#[async_trait]
impl<'a> Provider<'a, HashMap<AccessTokenId, Arc<Session>>, &'a mut SqliteConnection>
    for SqlSessionProvider
{
    async fn get(
        &'a self,
        conn: &'a mut SqliteConnection,
    ) -> Result<Arc<HashMap<AccessTokenId, Arc<Session>>>, TdError> {
        let status = &(&SessionStatus::Active);
        let mut query_builder = self.queries.select_by::<SessionDBWithNames>(status)?;

        // if we get a connection in the call we use it, else we use one from the DbPool
        // we have to acquire one regardless
        let sessions: Vec<Session> = query_builder
            .build_query_as()
            .fetch_all(conn)
            .await
            .map_err(handle_select_error)?;
        Ok(Arc::new(
            sessions
                .into_iter()
                .map(|s| (*s.access_token_id(), Arc::new(s)))
                .collect(),
        ))
    }

    async fn purge(&'a self, conn: &'a mut SqliteConnection) -> Result<(), TdError> {
        if self.time_to_purge() {
            let now = AtTime::now().await;
            let mut query = self.queries.delete_by::<SessionDB>(&())?;
            query.push(" WHERE expires_on < ");
            query.push_bind(&now);
            let purged = query
                .build()
                .execute(conn)
                .await
                .map_err(handle_delete_error)?
                .rows_affected() as i64;
            debug!("Purged {purged} expired sessions");
        }
        Ok(())
    }
}

#[async_trait]
impl SessionProvider<'_> for SqlSessionProvider {}

pub type Sessions = CachedProvider<
    'static,
    HashMap<AccessTokenId, Arc<Session>>,
    &'static mut SqliteConnection,
    SqlSessionProvider,
>;

#[async_trait]
impl<'a> SessionProvider<'a>
    for CachedProvider<
        'a,
        HashMap<AccessTokenId, Arc<Session>>,
        &'a mut SqliteConnection,
        SqlSessionProvider,
    >
{
    async fn get_session(
        &'a self,
        conn: &'a mut SqliteConnection,
        access_token_id: &AccessTokenId,
    ) -> Result<Arc<Session>, TdError> {
        self.get(conn)
            .await?
            .get(access_token_id)
            .ok_or_else(|| SessionError::NotFound(*access_token_id).into())
            .cloned()
    }
}
