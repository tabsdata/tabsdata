//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{Global, RW};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::service_token::ServiceTokenDB;
use td_objects::types::basic::{TokenSlot, UserId};
use td_queries::Queries;

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

/// System tokens are refused: no API removes one.
pub(crate) async fn delete_token_in_slot(
    conn: &mut DomainConnection<Apiserver, Global, RW>,
    user_id: &UserId,
    slot: &TokenSlot,
) -> Result<ServiceTokenDB, TdError> {
    let row: ServiceTokenDB = conn
        .fetch_optional(and!(user_id, slot))
        .await?
        .ok_or(TokenError::NotFound)?;
    if *row.system {
        Err(TokenError::SystemManaged)?
    }
    conn.delete::<ServiceTokenDB>(row.id).await?;
    Ok(row)
}
