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
    /// Metric row key in the JSONL output, e.g. `"metrics::per_table"`.
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
        covers: &["Unversioned::one"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_optional",
        // assert_exists / assert_not_exists are thin wrappers over the same
        // fetch_optional query.
        covers: &["Unversioned::one", "Unversioned::first"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_all",
        covers: &["Unversioned::all"],
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
        covers: &["Current::update"],
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
        covers: &["Unversioned::list"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "tags::keys",
        covers: &["Tags::keys"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "tags::values",
        covers: &["Tags::values"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_latest_before",
        covers: &["Unversioned::first"],
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
        covers: &["Unversioned::first"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_optional_for_update",
        covers: &["Locked::one"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_one_for_update",
        covers: &["Locked::one"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_all_for_update",
        covers: &["Locked::all"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::claim_all",
        covers: &["Locked::claim_all"],
        family: Family::Default,
        status: Status::Implemented,
    },
    // generic/scd2.rs — open-interval (at-now) reads and the interval writes.
    LoadCase {
        name: "latest::fetch_one",
        covers: &["Current::one"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_optional",
        covers: &["Current::one"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_all",
        covers: &["Current::all"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_all_chunked",
        covers: &["Current::chunked_all"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "last::fetch_all",
        covers: &["LastVersion::all"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "last::fetch_all_chunked",
        covers: &["LastVersion::chunked_all"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::list",
        covers: &["Current::list"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "tagged::list",
        covers: &["Current::list"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    // generic/at cases — partition-keyed lookups through FindAt.
    LoadCase {
        name: "as_of::fetch_one_by_partition",
        covers: &["VisibleAt::one"],
        family: Family::At,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_optional_by_partition",
        covers: &["VisibleAt::one"],
        family: Family::At,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_all_chunked_by_partition",
        covers: &["VisibleAt::chunked_all"],
        family: Family::At,
        status: Status::Implemented,
    },
    // generic/find_at.rs — cross-partition latest-at-time by condition.
    LoadCase {
        name: "as_of::fetch_one",
        covers: &["VisibleAt::one"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_optional",
        // fetch_optional and assert_not_exists are thin wrappers over the
        // same fetch_first query.
        covers: &["VisibleAt::first", "VisibleAt::one"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_all",
        covers: &["VisibleAt::all"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::fetch_all_chunked",
        covers: &["VisibleAt::chunked_all"],
        family: Family::AsOf,
        status: Status::Implemented,
    },
    LoadCase {
        name: "as_of::list",
        covers: &["VisibleAt::list"],
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
        name: "data_version::heads",
        covers: &["Heads::fetch_all"],
        family: Family::DataVersion,
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
        name: "lfr::awaiting_commit",
        covers: &["LogicalRuns::awaiting_commit"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::done_beside_canceled",
        covers: &["LogicalRuns::done_beside_canceled"],
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
        name: "lfr::of_deleted_functions",
        covers: &["LogicalRuns::of_deleted_functions"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "projects::draining",
        covers: &["Projects::draining"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::blockers",
        covers: &["LogicalRuns::blockers"],
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
        name: "lfr::set_status",
        covers: &["LogicalRuns::set_status", "LogicalRuns::set_status_if"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pfr::reported",
        covers: &[
            "PhysicalRuns::reported",
            "PhysicalRuns::ended",
            "PhysicalRuns::dispatched",
            "PhysicalRuns::end",
        ],
        family: Family::Latest,
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
        covers: &["Current::one"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    LoadCase {
        name: "attempts::current_version",
        covers: &["Current::one"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    LoadCase {
        name: "attempts::version_at",
        covers: &["VisibleAt::one"],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    // daos/table_data_versions.rs — PTDV-with-names, bound-interval filter.
    LoadCase {
        name: "ltdv::committed_data_at",
        covers: &[
            "TableDataVersions::committed_data_at",
            "TableDataVersions::data_including_uncommitted_at",
        ],
        family: Family::Attempts,
        status: Status::Implemented,
    },
    LoadCase {
        name: "ltdv::committed_data_before",
        covers: &["TableDataVersions::committed_data_before"],
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
        name: "metrics::per_table",
        covers: &["Metrics::per_table"],
        family: Family::Metrics,
        status: Status::Implemented,
    },
    // The v9 rung's own derivation over every project's base rows. The fact
    // writers derive exactly the same rows per unit of work (the DAO gate
    // holds them to it), so this case stands in for their growth too — they
    // have no load case of their own.
    LoadCase {
        name: "metrics::backfill",
        covers: &[
            "load_ended_attempts",
            "attempts_ended",
            "versions_consumed",
            "runs_status_changed",
            "runs_reattempted",
        ],
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
        covers: &["AllVersions::one"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "timeline::fetch_optional",
        covers: &["AllVersions::one"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_all",
        covers: &["AllVersions::all"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_first",
        covers: &["AllVersions::first"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_last",
        covers: &["AllVersions::first"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::list",
        covers: &["AllVersions::list"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::list_until",
        covers: &["Until::list"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "history::fetch_all_chunked",
        covers: &["AllVersions::chunked_all"],
        family: Family::History,
        status: Status::Implemented,
    },
    // generic/chunked.rs — the wordless grain's chunked operations, and the
    // chunked SCD2 close.
    LoadCase {
        name: "chunked::fetch_all",
        covers: &["Unversioned::chunked_all"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "chunked::insert",
        covers: &["Unversioned::chunked_insert"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::update_chunked",
        covers: &["Current::chunked_update"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "chunked::update",
        covers: &["Unversioned::chunked_update"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "chunked::close",
        covers: &["Current::chunked_close"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_first",
        covers: &["Unversioned::first"],
        family: Family::Default,
        status: Status::Implemented,
    },
    // The SCD2 close is a write against the open interval, timed under the
    // default family like the other writes.
    LoadCase {
        name: "default::close",
        covers: &["Current::close"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::open",
        covers: &["Current::open", "Topology::bump"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::open_chunked",
        covers: &["Current::chunked_open"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::pending_chunked",
        covers: &["TableDataVersions::pending"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "scd2::settle",
        covers: &["TableDataVersions::settle"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "latest::fetch_first",
        // assert_not_exists is a thin wrapper over the same fetch_first
        // query.
        covers: &["Current::first"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_first_for_update",
        covers: &["Locked::first"],
        family: Family::Default,
        status: Status::Implemented,
    },
    LoadCase {
        name: "default::fetch_all_for_update_chunked",
        covers: &["Locked::chunked_all"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
    LoadCase {
        name: "last::fetch_one",
        covers: &["LastVersion::one"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "last::fetch_first",
        covers: &["LastVersion::first"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "last::list",
        covers: &["LastVersion::list"],
        family: Family::Latest,
        status: Status::Implemented,
    },
    LoadCase {
        name: "until::fetch_one",
        covers: &["Until::one"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "until::fetch_first",
        covers: &["Until::first"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "until::fetch_all",
        covers: &["Until::all"],
        family: Family::History,
        status: Status::Implemented,
    },
    LoadCase {
        name: "until::fetch_all_chunked",
        covers: &["Until::chunked_all"],
        family: Family::Chunked,
        status: Status::Implemented,
    },
];
