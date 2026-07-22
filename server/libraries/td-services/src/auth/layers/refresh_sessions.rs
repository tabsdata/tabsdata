//
// Copyright 2025 Tabsdata Inc.
//

use td_authz::session_provider::Sessions;
use td_common::provider::Provider;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_tower::extractors::{MutRef, Ref};
use td_tower::post_commit::PostCommitActions;

/// Keep the process-wide session cache correct across a session change
/// (login / refresh / logout).
///
/// The cache is a snapshot of all active sessions, shared by every request.
/// It must be invalidated *after* this transaction commits: only then is the
/// session this transaction is writing visible to the fresh connections other
/// requests read on. Invalidating inline (before commit) races the commit — a
/// concurrent reader can repopulate the cache from a pre-commit snapshot that
/// omits the new session, and that stale snapshot then sticks until the next
/// invalidation, so every request bearing the just-issued access token is
/// rejected with `SessionError::NotFound` (HTTP 401, "Authentication failed").
///
/// So we only do the throttled housekeeping purge of expired rows here (its
/// deletes commit with this transaction), and defer the authoritative cache
/// invalidation to a post-commit action.
pub async fn refresh_sessions(
    sessions: Ref<Sessions>,
    mut conn: MutRef<DomainConnection<Apiserver>>,
    mut post_commit: MutRef<PostCommitActions>,
) -> Result<(), TdError> {
    // Housekeeping only: delete expired session rows (throttled, ~hourly).
    sessions.purge(&mut *conn).await?;

    // Authoritative invalidation, deferred until the transaction commits.
    let sessions = sessions.arc();
    post_commit.push(move || Box::pin(async move { sessions.invalidate().await }));
    Ok(())
}
