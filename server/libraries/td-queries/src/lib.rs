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
pub mod metrics_facts {
    pub use crate::daos::metrics::write::runs_reattempted;
}
pub use daos::session::Sessions;
pub use daos::table_data_versions::TableDataVersions;

pub use executor::{QueryExecutor, Serves, Writes, WritesTo};
pub use generic::chunked::Chunked;
pub use generic::data_version::{DataVersion, Heads};
pub use generic::grain::{
    AllVersions, Current, Grain, LastVersion, Locked, OneRow, Until, Unversioned, VisibleAt,
};
pub use generic::reader::Reader;
pub use generic::tagged::TagFilter;
pub use primitives::listing::list_plan_sql;

pub use logic_id::initialize_logical_ids;
pub use queries::Queries;

#[cfg(feature = "test-utils")]
pub mod fixtures;

#[cfg(feature = "test-utils")]
pub mod test_utils;
