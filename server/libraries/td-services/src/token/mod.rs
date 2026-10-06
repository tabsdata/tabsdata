//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

pub mod expiry;
pub mod services;

#[td_error]
pub enum TokenError {
    #[error(
        "A token's lifetime is a positive number of days, or -1 for a token that never expires"
    )]
    LifetimeIsNotADuration = 0,
    #[error("This deployment does not permit tokens that never expire")]
    NeverExpiringNotPermitted = 1,
    #[error("A token may last at most {0} days here")]
    AboveMaxDays(u32) = 2,
    #[error("A token's lifetime of {0} days reaches past the last representable instant")]
    LifetimeOutOfRange(u32) = 3,

    #[error("Token not found")]
    NotFound = 1000,

    #[error("Service tokens are not configured on this deployment")]
    NotConfigured = 2000,
    #[error("This token is managed by the server; rotate its secret to replace it")]
    SystemManaged = 2001,
}
