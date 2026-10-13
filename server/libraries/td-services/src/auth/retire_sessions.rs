//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{DbScope, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::auth::SessionInvalidateDB;
use td_objects::types::basic::{AtTime, SessionStatus, UserId};
use td_queries::Queries;

/// Ends every Active session of `user` with `status`, in the same unit of
/// work as the change that takes the user's access away. Sessions already
/// ended keep the status they ended with.
pub async fn retire_sessions<S: DbScope, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    user: &UserId,
    at: AtTime,
    status: SessionStatus,
) -> Result<(), TdError> {
    let retire = SessionInvalidateDB::new(at, status);
    conn.update(&retire, and!(user, SessionStatus::Active))
        .await?;
    Ok(())
}
