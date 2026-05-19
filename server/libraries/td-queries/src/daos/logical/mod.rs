//
// Copyright 2026 Tabs Data Inc.
//

//! Logic-version layer: pointer-event primitives, state machine, and
//! temporal-view reads for LFR / LTDV identities, plus the shared SQL
//! scaffold for readiness/commit/blocked queries.

pub mod cascade;
pub(crate) mod chain;
pub mod pointer;
pub mod runs;

pub use cascade::{LogicalCascade, LogicalCascadeEntry};
pub use pointer::{
    LogicalPointers, LogicalPointersEntry, ptdv_is_canonical_committed, redirect_event, void_event,
};
pub use runs::{LogicalRuns, LogicalRunsEntry};
