//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt::{Debug, Formatter};
use td_common::datetime::Timestamp;

/// Proof that a lock was taken, carrying the instant it was: minted by
/// [`XW`](crate::access::XW) once it holds the project's advisory lock and by
/// `serialize_on` once its `FOR UPDATE` read returns. The only instant an
/// SCD2 interval opens or closes at, so stamps follow lock order and never a
/// request's arrival time, which may predate rows a lock holder ahead in the
/// queue committed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LockInstant(Timestamp);

impl LockInstant {
    pub(crate) fn now() -> Self {
        Self(Timestamp::default())
    }

    /// For `serialize_on` alone, called once its locking read has returned.
    /// Any other caller is a review finding: grep for it.
    pub fn after_row_lock() -> Self {
        Self::now()
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
