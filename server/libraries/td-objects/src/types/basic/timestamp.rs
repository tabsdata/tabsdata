//
// Copyright 2025 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use td_common::datetime::{IntervalEnd, Timestamp};
use td_database::access::LockInstant;

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AtTime(Timestamp);

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
#[typed(try_from = AtTime)]
pub struct PasswordChangeTime(Timestamp);

/// The instant a plan was triggered: the lock instant of the unit of work
/// that wrote the row, carried onto every row of the plan and its runs, so
/// data versions sort by it in lock order. Built from a [`LockInstant`] and
/// nothing else.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(sealed)]
pub struct TriggeredOn(Timestamp);

impl TriggeredOn {
    /// Tests stand in for the lock; every use is an audit point.
    #[cfg(feature = "td-test")]
    pub fn asserted(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }
}

impl From<LockInstant> for TriggeredOn {
    fn from(instant: LockInstant) -> Self {
        Self(instant.timestamp())
    }
}

impl From<TriggeredOn> for AtTime {
    fn from(at: TriggeredOn) -> Self {
        Self(at.0)
    }
}

impl From<LockInstant> for AtTime {
    fn from(instant: LockInstant) -> Self {
        Self(instant.timestamp())
    }
}

/// The SCD2 interval start, bound by `open(.., at)` from the lock instant;
/// projected by row DAOs, never carried by a payload, never built in
/// process.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(sealed)]
pub struct Scd2ValidFrom(Timestamp);

impl Scd2ValidFrom {
    /// Tests stand in for the door; every use is an audit point.
    #[cfg(feature = "td-test")]
    pub fn asserted(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }
}

impl From<Scd2ValidFrom> for AtTime {
    fn from(at: Scd2ValidFrom) -> Self {
        Self(at.0)
    }
}

/// The SCD2 interval end, the close-out trigger's and `close(.., at)`'s;
/// projected by row DAOs, never built in process.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[typed(sealed)]
pub struct Scd2ValidTo(IntervalEnd);

impl Scd2ValidTo {
    /// Tests stand in for the trigger; every use is an audit point.
    #[cfg(feature = "td-test")]
    pub fn infinity() -> Self {
        Self(IntervalEnd::Infinity)
    }

    pub fn is_infinity(&self) -> bool {
        self.0.is_infinity()
    }
}

/// `infinity` for a token that never expires; never NULL.
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub struct TokenExpiresOn(IntervalEnd);

impl TokenExpiresOn {
    pub fn never() -> Self {
        Self(IntervalEnd::Infinity)
    }

    pub fn is_live_at(&self, at: &AtTime) -> bool {
        match self.0.at() {
            Some(end) => at.0 < *end,
            None => true,
        }
    }
}
