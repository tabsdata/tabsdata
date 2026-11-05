//
// Copyright 2026 Tabsdata Inc.
//

//! Independent items built inside one unit of a pass: an item whose build
//! fails is parked with its error and the rest are built, so one row never
//! stops its project's pass. What becomes of a parked item is the step's:
//! left as it is for the next pass, or ended.

use crate::scheduler::stats;
use td_error::TdError;
use tracing::warn;

pub struct Parked<I> {
    pub item: I,
    pub error: TdError,
}

pub struct Partial<T, I> {
    pub built: Vec<T>,
    pub parked: Vec<Parked<I>>,
}

impl<T, I> Default for Partial<T, I> {
    fn default() -> Self {
        Self {
            built: Vec::new(),
            parked: Vec::new(),
        }
    }
}

impl<T, I> Partial<T, I> {
    pub fn build(
        items: impl IntoIterator<Item = I>,
        mut build: impl FnMut(&I) -> Result<T, TdError>,
    ) -> Self {
        let mut partial = Self::default();
        for item in items {
            match build(&item) {
                Ok(built) => partial.built.push(built),
                Err(error) => partial.park(item, error),
            }
        }
        partial
    }

    pub fn park(&mut self, item: I, error: TdError) {
        self.parked.push(Parked { item, error });
    }

    /// The built items; every parked one is left as it is, for the next
    /// pass, and logged once under `step`.
    pub fn left_for_next_pass(self, step: &str, id: impl Fn(&I) -> String) -> Vec<T> {
        if !self.parked.is_empty() {
            stats::parked(step, self.parked.len() as u64);
        }
        for parked in &self.parked {
            warn!(
                step,
                item = %id(&parked.item),
                error = ?parked.error,
                "scheduler.pass: item parked, left for the next pass"
            );
        }
        self.built
    }
}
