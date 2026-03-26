//
// Copyright 2025. Tabs Data Inc.
//

use td_authz::session_provider::Sessions;
use td_common::provider::Provider;
use td_error::TdError;
use td_database::dynamic::DynamicConnection;
use td_tower::extractors::{MutRef, Ref};

pub async fn refresh_sessions(
    sessions: Ref<Sessions>,
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<(), TdError> {
    sessions.purge(&mut conn).await?; // erases expired sessions
    sessions.get(&mut conn).await?; // force the reload of the cache
    Ok(())
}
