//
// Copyright 2025 Tabsdata Inc.
//

mod executor;
mod queries;

mod daos;
mod generic;
mod logic_id;
mod primitives;

pub use daos::dependency::Dependencies;
pub use daos::execution::Executions;
pub use daos::function_runs::{LogicalRuns, PhysicalRuns};
pub use daos::metrics::Metrics;
#[cfg(feature = "td-test")]
pub use daos::metrics::metrics_plan_sql;
pub use daos::session::Sessions;
pub use daos::table_data_versions::TableDataVersions;

pub use executor::{QueryExecutor, Serves};
pub use generic::as_of::AsOf;
pub use generic::chunked::{AsOfChunked, Chunked, Scd2Chunked, TimelineChunked};
pub use generic::data_version::DataVersion;
pub use generic::for_update::ForUpdate;
pub use generic::timeline::Timeline;

pub use generic::recursive::{AsOfRecursive, Scd2Recursive};
pub use generic::scd2::Scd2;

pub use logic_id::initialize_logical_ids;
pub use queries::Queries;

#[cfg(feature = "test-utils")]
pub mod fixtures;

#[cfg(feature = "test-utils")]
pub mod test_utils;
