//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::session::Sessions;
use td_common::provider::Provider;
use td_error::TdError;
use td_tower::extractors::{Connection, IntoMutSqlConnection, SrvCtx};

pub async fn refresh_sessions(
    SrvCtx(sessions): SrvCtx<Sessions>,
    Connection(conn): Connection,
) -> Result<(), TdError> {
    let mut conn_ = conn.lock().await;
    let mut conn = conn_.get_mut_connection()?;
    sessions.purge(&mut conn).await?; // erases expired sessions
    sessions.get(&mut conn).await?; // force the reload of the cache
    Ok(())
}
