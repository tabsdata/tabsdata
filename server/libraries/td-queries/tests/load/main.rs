//
// Copyright 2026 Tabsdata Inc.
//

//! Query load-test suite: every public query in `td-queries` has a case that
//! seeds a sized dataset and times one cold execution. [`cases::LOAD_CASES`]
//! enumerates every query, [`audit`] fails on drift from the query surface,
//! [`scaling`] guards growth at two sizes, [`report`] checks runtime coverage
//! and regression. See `tests/load/README.md`.

mod at_cases;
mod attempt_cases;
mod audit;
mod bulk;
mod cases;
mod chain_cases;
mod chunked_cases;
mod dataset;
mod dataversion_cases;
mod default_write_cases;
mod harness;
mod history_cases;
mod indexes;
mod latest_default_cases;
mod list_cases;
mod metric;
mod metrics_cases;
mod profile;
mod raw;
mod recursive_cases;
mod report;
mod scaling;
mod scd2_cases;
mod status_door_cases;
