//
// Copyright 2026 Tabsdata Inc.
//

//! Function-run queries: [`LogicalRuns`] over the LFR identity table,
//! [`PhysicalRuns`] over the attempt table. Each submodule attaches the
//! methods of one concern to the handle it belongs to.

mod attempts;
pub(crate) mod chain;
mod dispatch;
mod functions;
mod readiness;
mod settlement;
mod status;

/// LFR-rooted run queries: readiness, commit, dispatch-time enrichment.
pub struct LogicalRuns<E> {
    pub(crate) executor: E,
}

/// PFR-rooted run queries: replay dispatch, the stale-dispatch sweep.
pub struct PhysicalRuns<E> {
    pub(crate) executor: E,
}
