//
// Copyright 2026 Tabsdata Inc.
//

use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
use td_common::datetime::IntervalEnd;

/// The trigger-owned SCD2 `valid_to` column, surfaced on read-only view
/// DAOs; regular DAOs never carry it (inserts must not bind it).
#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Scd2ValidTo(IntervalEnd);

impl Scd2ValidTo {
    pub fn infinity() -> Self {
        Self(IntervalEnd::Infinity)
    }

    pub fn is_infinity(&self) -> bool {
        self.0.is_infinity()
    }
}
