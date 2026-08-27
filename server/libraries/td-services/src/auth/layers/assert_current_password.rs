//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::types::basic::PasswordHash;
use td_security::password;
use td_tower::extractors::Ref;
use tracing::error;

pub async fn assert_current_password<P: Deref<Target = String>>(
    password_hash: Ref<PasswordHash>,
    password: Ref<P>,
) -> Result<(), TdError> {
    password::verify_password(password_hash.as_str(), password.deref())
        .then_some(())
        .ok_or_else(|| {
            error!("Authentication failed: invalid password");
            AuthError::AuthenticationFailed.into()
        })
}
