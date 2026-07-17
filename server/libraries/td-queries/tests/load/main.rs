//
// Copyright 2026 Tabs Data Inc.
//

//! Query load-test suite.
//!
//! Goal: every public query in `td-queries` has a load case that seeds a
//! large, *specialized* dataset (only the tables the query reads, sized on
//! the axis that query scales with) and times one cold execution against
//! each backend.
//!
//! This commit lands the harness: the [`cases::LOAD_CASES`] registry
//! enumerates every query and the [`audit`] test fails if the registry ever
//! drifts from the crate's public query surface; [`profile`] is the size dial,
//! [`dataset`]/[`bulk`] the seeders, [`harness`] the shared scaffolding, and
//! [`metric`]/[`report`] the JSONL capture and post-run analysis. The timed
//! per-query invocations land with the case families.
//!
//! See `tests/load/README` for the size profiles and metric-output format.

mod audit;
mod bulk;
mod cases;
mod dataset;
mod harness;
mod metric;
mod profile;
mod report;
