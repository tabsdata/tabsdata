//
// Copyright 2025 Tabsdata Inc.
//

use td_database::domain::Apiserver;
use td_error::{TdError, td_error};
use td_objects::dxo::auth::SessionDBWithNames;
use td_objects::types::basic::{AccessTokenId, SessionStatus};
use td_queries::{Queries, QueryExecutor};

pub type Session = SessionDBWithNames;

#[td_error]
pub enum SessionError {
    #[error("Session not found: {0}")]
    NotFound(AccessTokenId) = 4000,
}

/// The session `access_token_id` names, if it is Active.
pub async fn active_session(
    conn: impl QueryExecutor<Domain = Apiserver>,
    access_token_id: &AccessTokenId,
) -> Result<Option<Session>, TdError> {
    let session: Option<Session> = conn.fetch_optional(access_token_id).await?;
    Ok(session.filter(|session| session.base.status == SessionStatus::Active))
}
