//
// Copyright 2026 Tabsdata Inc.
//

//! The load-case registry: one [`LoadCase`] per public query in `td-queries`.
//! [`crate::audit`] cross-checks `covers` against the crate's actual query
//! surface, so this slice cannot silently fall behind a newly added query.

/// Which sized fixture a case reads. All families except `DataVersion` and
/// `Metrics` reuse the chain dataset and differ in which of its tables/axes
/// they stress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    /// The wordless grain: unversioned single-table reads and writes.
    Default,
    /// At-now reads over the open SCD2 intervals.
    Latest,
    /// Partition-scoped point-in-time, one partition.
    At,
    /// Cross-partition point-in-time by condition.
    AsOf,
    /// `Versions`-selector reads over the purpose-built `LoadDvDao` fixture,
    /// sized on versions-per-partition.
    DataVersion,
    /// Recursive walk over the producer→consumer dependency edges. Recursion
    /// depth stays 1 and fan-out 1: that axis is an open coverage gap.
    Recursive,
    /// Logical-chain walk: `LFR ⨝ LFRI ⨝ LTDV ⨝ bound attempt ⨝ producer`.
    Chain,
    /// The current attempt of a logical row.
    Attempts,
    /// Every version row a condition matches, with no time predicate.
    History,
    /// The same operations over a slice that may exceed the bind-parameter
    /// limit.
    Chunked,
    /// Aggregation over the `__with_names` views — the metrics dataset.
    Metrics,
    Sessions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Placeholder for a query added ahead of its case; nothing constructs it
    /// today.
    #[allow(dead_code)]
    Todo,
    Implemented,
}

pub struct LoadCase {
    /// Metric row key in the JSONL output, e.g. `"metrics::metrics_edges"`.
    pub name: &'static str,
    /// The `Struct::method` identifiers this case exercises; matched by the
    /// audit against the crate's `pub async fn`s.
    pub covers: &'static [&'static str],
    pub family: Family,
    pub status: Status,
}

/// Grouped by source file; order is not significant.
pub const LOAD_CASES: &[LoadCase] = &[
    // The wordless grain — single-table CRUD (writes scale with table + index size too).
    LoadCase {
        name: "default::insert",
        covers: &["Queries::insert"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_one",
        covers: &["Queries::fetch_one"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_optional",
        // assert_exists / assert_not_exists are thin wrappers over the same
        // fetch_optional query.
        covers: &[
            "Queries::fetch_optional",
            "Queries::assert_exists",
            "Queries::assert_not_exists",
        ],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_all",
        covers: &["Queries::fetch_all"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::update",
        covers: &["Queries::update"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::update",
        covers: &["Scd2::update"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::delete",
        covers: &["Queries::delete"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::list",
        covers: &["Queries::list"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_latest_before",
        covers: &["Queries::fetch_nth_last"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::upsert",
        covers: &["Queries::upsert"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::insert_or_ignore",
        covers: &["Queries::insert_or_ignore"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_last",
        covers: &["Queries::fetch_last"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_optional_for_update",
        covers: &["ForUpdate::fetch_optional"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_one_for_update",
        covers: &["ForUpdate::fetch_one"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_all_for_update",
        covers: &["ForUpdate::fetch_all"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::claim_all",
        covers: &["ForUpdate::claim_all"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::serialize_on",
        covers: &["ForUpdate::serialize_on"],
        family: Family::Default,
        status: Status::Implemented,
    },
    // generic/scd2.rs — open-interval (at-now) reads and the interval writes.
    LoadCase {
        name: "latest::fetch_one",
        covers: &["Scd2::fetch_one"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_optional",
        covers: &["Scd2::fetch_optional"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_all",
        covers: &["Scd2::fetch_all"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_all_chunked",
        covers: &["Scd2Chunked::fetch_all"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::list",
        covers: &["Scd2::list"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    // generic/at cases — partition-keyed lookups through FindAt.
    LoadCase {
        name: "as_of::fetch_one_by_partition",
        covers: &["AsOf::fetch_one"],
        family: Family::At,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_optional_by_partition",
        covers: &["AsOf::fetch_optional"],
        family: Family::At,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_all_chunked_by_partition",
        covers: &["AsOfChunked::fetch_all"],
        family: Family::At,
        status: Status::Implemented,
    },
    // generic/find_at.rs — cross-partition latest-at-time by condition.
    LoadCase {
        name: "as_of::fetch_one",
        covers: &["AsOf::fetch_one"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_optional",
        // fetch_optional and assert_not_exists are thin wrappers over the
        // same fetch_first query.
        covers: &[
            "AsOf::fetch_first",
            "AsOf::fetch_optional",
            "AsOf::assert_not_exists",
        ],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_all",
        covers: &["AsOf::fetch_all"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_all_chunked",
        covers: &["AsOfChunked::fetch_all"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::list",
        covers: &["AsOf::list"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    // generic/data_version.rs — Versions-selector reads (head/initial/range).
    LoadCase {
        name: "data_version::fetch_one",
        covers: &["DataVersion::fetch_one"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "data_version::fetch_optional",
        covers: &["DataVersion::fetch_optional"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "data_version::fetch_all",
        covers: &["DataVersion::fetch_all"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "data_version::fetch_relative_offset",
        covers: &["DataVersion::fetch_relative_offset"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "recursive::current",
        covers: &["Scd2Recursive::fetch_all"],
        family: Family::Recursive,
        status: Status::Implemented,
    },
    LoadCase {
        name: "recursive::find_at",
        covers: &["AsOfRecursive::fetch_all"],
        family: Family::Recursive,
        status: Status::Implemented,
    },
    // daos/function_runs.rs — logical-chain readiness/commit/dispatch.
    LoadCase {
        name: "lfr::to_execute",
        covers: &["LogicalRuns::to_execute"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::to_commit",
        covers: &["LogicalRuns::to_commit"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::previous_for_function_at",
        covers: &["LogicalRuns::previous_for_function_at"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::for_dispatch",
        covers: &["LogicalRuns::for_dispatch"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::blocked_ids",
        covers: &["LogicalRuns::blocked_ids"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::downstream_one_hop",
        covers: &["LogicalRuns::downstream_one_hop"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::effective_output_base_paths",
        covers: &["LogicalRuns::effective_output_base_paths"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::with_bundle_base_path",
        covers: &["LogicalRuns::with_bundle_base_path"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pfr::to_dispatch",
        covers: &["PhysicalRuns::to_dispatch"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pfr::dispatched_before",
        covers: &["PhysicalRuns::dispatched_before"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    // daos/execution.rs + daos/dependency.rs — chain-adjacent specialized.
    LoadCase {
        name: "executions::list_by_table_data_version",
        covers: &["Executions::list_by_table_data_version"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "executions::list_by_function_version",
        covers: &["Executions::list_by_function_version"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "dependencies::active_for_function_at",
        covers: &["Dependencies::active_for_function_at"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    // The attempt tables' current-version reads.
    LoadCase {
        name: "attempts::current_run",
        covers: &["Scd2::fetch_optional"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    LoadCase {
        name: "attempts::current_version",
        covers: &["Scd2::fetch_optional"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    LoadCase {
        name: "attempts::version_at",
        covers: &["AsOf::fetch_optional"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    // daos/table_data_versions.rs — PTDV-with-names, bound-interval filter.
    LoadCase {
        name: "ltdv::latest_bound_tdv_with_data_for_table",
        covers: &["TableDataVersions::latest_bound_tdv_with_data_for_table"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    // daos/metrics.rs — heaviest aggregations.
    LoadCase {
        name: "metrics::inflight",
        covers: &["Metrics::inflight"],
        family: Family::Metrics,
        status: Status::Implemented,
    },
    LoadCase {
        name: "metrics::metrics_nodes",
        covers: &["Metrics::metrics_nodes"],
        family: Family::Metrics,
        status: Status::Implemented,
    },
    LoadCase {
        name: "metrics::metrics_edges",
        covers: &["Metrics::metrics_edges"],
        family: Family::Metrics,
        status: Status::Implemented,
    },
    LoadCase {
        name: "metrics::metrics_node_output_tables",
        covers: &["Metrics::metrics_node_output_tables"],
        family: Family::Metrics,
        status: Status::Implemented,
    },
    LoadCase {
        name: "metrics::metrics_edge_tables",
        covers: &["Metrics::metrics_edge_tables"],
        family: Family::Metrics,
        status: Status::Implemented,
    },
    // daos/session.rs — session table purge.
    LoadCase {
        name: "sessions::purge",
        covers: &["Sessions::purge"],
        family: Family::Sessions,
        status: Status::Implemented,
    },
    // generic/timeline.rs — the version log of a partition, no validity
    // filter, one version by id, and the time-bounded listing over it.
    LoadCase {
        name: "timeline::fetch_one",
        covers: &["Timeline::fetch_one"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "timeline::fetch_optional",
        covers: &["Timeline::fetch_optional"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_all",
        covers: &["Timeline::fetch_all"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_first",
        covers: &["Timeline::fetch_first"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_last",
        covers: &["Timeline::fetch_last"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::list",
        covers: &["Timeline::list"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::list_until",
        covers: &["Timeline::list_until"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_all_chunked",
        covers: &["TimelineChunked::fetch_all"],
        family: Family::History,
        status: Status::Implemented,
    },
    // generic/chunked.rs — the wordless grain's chunked operations, and the
    // chunked SCD2 close.
    LoadCase {
        name: "chunked::fetch_all",
        covers: &["Chunked::fetch_all"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "chunked::insert",
        covers: &["Chunked::insert"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::update_chunked",
        covers: &["Scd2Chunked::update"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "chunked::update",
        covers: &["Chunked::update"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "chunked::close",
        covers: &["Scd2Chunked::close"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_first",
        covers: &["Queries::fetch_first"],
        family: Family::Default,
        status: Status::Implemented,
    },
    // The SCD2 close is a write against the open interval, timed under the
    // default family like the other writes.
    LoadCase {
        name: "default::close",
        covers: &["Scd2::close"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::open",
        covers: &["Scd2::open"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::open_chunked",
        covers: &["Scd2Chunked::open"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_first",
        // assert_not_exists is a thin wrapper over the same fetch_first
        // query.
        covers: &["Scd2::fetch_first", "Scd2::assert_not_exists"],
        family: Family::Latest,
        status: Status::Implemented,
    },
];
