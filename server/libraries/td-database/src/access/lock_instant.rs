//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt::{Debug, Formatter};
use td_common::datetime::Timestamp;

/// The instant a write unit writes at, minted once it holds its lock: the only
/// one an SCD2 interval opens or closes at, so it never predates what it read.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LockInstant(Timestamp);

impl LockInstant {
    pub(crate) fn now() -> Self {
        Self(Timestamp::default())
    }

    /// Tests stand in for the lock; every use is an audit point.
    #[cfg(feature = "td-test")]
    pub fn asserted(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    pub fn timestamp(&self) -> Timestamp {
        self.0
    }
}

impl Debug for LockInstant {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LockInstant({})", self.0)
    }
}
