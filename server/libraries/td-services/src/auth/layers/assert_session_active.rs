//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::auth::SessionDB;
use td_objects::types::basic::SessionStatus;
use td_tower::extractors::Ref;

pub async fn assert_session_active(session: Ref<SessionDB>) -> Result<(), TdError> {
    if session.status == SessionStatus::Active {
        Ok(())
    } else {
        Err(AuthError::InvalidRefreshToken)?
    }
}
