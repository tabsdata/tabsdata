//
// Copyright 2025 Tabsdata Inc.
//

// `executor` first — its `query_handle!` macro is used by `daos/` and
// `generic/` modules.
#[macro_use]
mod executor;

mod daos;
mod generic;
mod logic_id;
mod primitives;

// Per-DAO query surfaces — wrapper struct + entry trait colocated in each
// module via the `query_handle!` macro.
pub use daos::dependency::{Dependencies, DependenciesEntry};
pub use daos::execution::{Executions, ExecutionsEntry};
#[cfg(feature = "td-test")]
pub use daos::function_runs::chain_plan_sql;
pub use daos::function_runs::{Lfr, LfrEntry, Pfr, PfrEntry};
#[cfg(feature = "td-test")]
pub use daos::metrics::metrics_plan_sql;
pub use daos::metrics::{Metrics, MetricsEntry};
pub use daos::pointers::{Bind, LogicalPointers, OutputRedirect, ReplayRedirect};
pub use daos::session::{Sessions, SessionsEntry};
pub use daos::table_data_versions::{Ltdv, LtdvEntry};

// Generic surfaces — independent of any specific DAO.
pub use executor::QueryExecutor;
pub use generic::chunked::ChunkedQueries;
pub use generic::current::{Current, CurrentEntry};
pub use generic::data_version::VersionedDataVersion;
pub use generic::historic::{Historic, HistoricEntry};
pub use generic::live::{Live, LiveEntry};
pub use generic::recursive::{CurrentRecursive, HistoricRecursive, RecursiveEntry, RecursiveQuery};
pub use generic::versioned::{Versioned, VersionedEntry};

pub use logic_id::initialize_logical_ids;

/// One-shot import of every entry trait. Standard caller boilerplate is
/// `use td_queries::prelude::*;` — brings in every entry method
/// (`live`/`current`/`versioned`/`historic` and every per-DAO `<surface>()`)
/// plus the `QueryExecutor` marker.
pub mod prelude {
    pub use super::{
        ChunkedQueries, CurrentEntry, DependenciesEntry, ExecutionsEntry, HistoricEntry, LfrEntry,
        LiveEntry, LtdvEntry, MetricsEntry, PfrEntry, QueryExecutor, RecursiveEntry, SessionsEntry,
        VersionedEntry,
    };
}

#[cfg(feature = "test-utils")]
pub mod fixtures;

#[cfg(feature = "test-utils")]
pub mod test_utils;
