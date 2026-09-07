//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::{Scd2ValidFrom, Scd2ValidTo};
use std::ops::Deref;
use ta_objects::IntervalColumns;
use td_common::datetime::Timestamp;

/// The interval an SCD2 row DAO projects, `[valid_from, valid_to)`: both
/// columns are written by the doors (`open(.., at)` and the close-out
/// trigger), never by a payload.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Scd2Interval {
    pub valid_from: Scd2ValidFrom,
    pub valid_to: Scd2ValidTo,
}

impl Scd2Interval {
    /// An open interval from `valid_from`: what a row DAO a test builds in
    /// memory, never written, carries.
    #[cfg(feature = "td-test")]
    pub fn open(valid_from: Scd2ValidFrom) -> Self {
        Self {
            valid_from,
            valid_to: Scd2ValidTo::infinity(),
        }
    }

    /// Whether the row was the live version at `at`, whichever timestamp
    /// newtype carries it.
    pub fn covers<V: Deref<Target = Timestamp>>(&self, at: &V) -> bool {
        *self.valid_from <= **at && self.valid_to.at().is_none_or(|end| **at < *end)
    }
}

impl IntervalColumns for Scd2Interval {
    type ValidFrom = Scd2ValidFrom;
    type ValidTo = Scd2ValidTo;
}
