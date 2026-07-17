//
// Copyright 2026 Tabs Data Inc.
//

//! Query load-test suite: every public query in `td-queries` has a case that
//! seeds a sized dataset and times one cold execution. [`cases::LOAD_CASES`]
//! enumerates every query, [`audit`] fails on drift from the query surface,
//! [`scaling`] guards growth at two sizes, [`report`] checks runtime coverage
//! and regression. See `tests/load/README.md`.

mod audit;
mod bulk;
mod cases;
mod chain_cases;
mod current_live_cases;
mod dataset;
mod dataversion_cases;
mod harness;
mod list_cases;
mod live_write_cases;
mod metric;
mod metrics_cases;
mod pointer_cases;
mod profile;
mod recursive_cases;
mod report;
mod scaling;
mod versioned_cases;
