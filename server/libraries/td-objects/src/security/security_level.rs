//
// Copyright 2025 Tabs Data Inc.
//

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use td_utoipa::api_server_schema;

/// Data Security Level. 0 is the highest security level.
#[api_server_schema]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SecurityLevel(u16);

impl SecurityLevel {
    pub fn or_system_default(level: &Option<SecurityLevel>) -> SecurityLevel {
        level.unwrap_or(10.into())
    }

    pub fn system_default() -> SecurityLevel {
        10.into()
    }
}

impl From<u16> for SecurityLevel {
    fn from(level: u16) -> Self {
        SecurityLevel(level)
    }
}

impl From<SecurityLevel> for u16 {
    fn from(level: SecurityLevel) -> u16 {
        level.0
    }
}

impl Display for SecurityLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
