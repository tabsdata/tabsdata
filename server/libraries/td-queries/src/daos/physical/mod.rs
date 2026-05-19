//
// Copyright 2026 Tabs Data Inc.
//

//! Physical-layer queries (PFR + PTDV). Sibling of [`super::logical`];
//! the two layers answer different questions:
//!
//! - `logical/` walks the pointer-event log and the LFR/LTDV identity
//!   rows. Used by readiness, commit, cascade, and pointer-state
//!   transitions.
//! - `physical/` walks the concrete attempt rows directly. Used when
//!   the dispatch trigger lives on the physical side (replay, future
//!   operator-rebound attempts) — the pointer log isn't the source of
//!   truth for "what needs a worker."

pub mod runs;

pub use runs::{PhysicalRuns, PhysicalRunsEntry};
