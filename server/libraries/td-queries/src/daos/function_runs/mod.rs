//
// Copyright 2026 Tabsdata Inc.
//

//! Function-run queries: [`LogicalRuns`] over the LFR identity table,
//! [`PhysicalRuns`] over the attempt table. Each submodule attaches the
//! methods of one concern to the handle it belongs to.

mod attempts;
mod blockers;
pub(crate) mod chain;
pub(crate) mod deleted_functions;
mod dispatch;
mod functions;
pub(crate) mod gates;
mod readiness;
mod settlement;
mod status;

/// LFR-rooted run queries: readiness, commit, dispatch-time enrichment.
pub struct LogicalRuns<E> {
    pub(crate) executor: E,
}

/// How far one downstream hop reaches from a set of runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reach {
    /// The runs reading a table of those runs: a yank and a cancel follow
    /// the data.
    Data,
    /// Also the subscribers that have not started and wait for those runs'
    /// transactions to commit, whichever of their tables they read: a hold
    /// and its release follow the commit.
    Commit,
}

/// PFR-rooted run queries: replay dispatch, the stale-dispatch sweep.
pub struct PhysicalRuns<E> {
    pub(crate) executor: E,
}
