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
    Live,
    /// `_current` view over a versioned table.
    Current,
    /// Partition-scoped point-in-time (ranked CTE), one partition.
    Versioned,
    /// Cross-partition ranked-CTE scan (the slow path).
    Historic,
    /// `Versions`-selector reads over the purpose-built `LoadDvDao` fixture,
    /// sized on versions-per-partition.
    DataVersion,
    /// Recursive walk over the producer→consumer dependency edges. Recursion
    /// depth stays 1 and fan-out 1: that axis is an open coverage gap.
    Recursive,
    /// Logical-chain walk: `LFR ⨝ LFRI ⨝ LTDV ⨝ latest-pointer ⨝ producer`.
    Chain,
    /// Pointer event log.
    Pointer,
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
    // generic/live.rs — single-table CRUD (writes scale with table + index size too).
    LoadCase {
        name: "live::insert",
        covers: &["Live::insert"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_one",
        covers: &["Live::fetch_one"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_optional",
        covers: &["Live::fetch_optional"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_all",
        covers: &["Live::fetch_all"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::update",
        covers: &["Live::update"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::delete",
        covers: &["Live::delete"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::list",
        covers: &["Live::list"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_all_in",
        covers: &["Live::fetch_all_in"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_latest_before",
        covers: &["Live::fetch_latest_before"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::upsert",
        covers: &["Live::upsert"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::insert_or_ignore",
        covers: &["Live::insert_or_ignore"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_last",
        covers: &["Live::fetch_last"],
        family: Family::Live,
        status: Status::Implemented,
    },
    LoadCase {
        name: "live::fetch_optional_for_update",
        covers: &["Live::fetch_optional_for_update"],
        family: Family::Live,
        status: Status::Implemented,
    },
    // generic/current.rs — `_current` view reads.
    LoadCase {
        name: "current::fetch_one",
        covers: &["Current::fetch_one"],
        family: Family::Current,
        status: Status::Implemented,
    },
    LoadCase {
        name: "current::fetch_optional",
        covers: &["Current::fetch_optional"],
        family: Family::Current,
        status: Status::Implemented,
    },
    LoadCase {
        name: "current::fetch_all",
        covers: &["Current::fetch_all"],
        family: Family::Current,
        status: Status::Implemented,
    },
    LoadCase {
        name: "current::fetch_all_chunked",
        covers: &["Current::fetch_all_chunked"],
        family: Family::Current,
        status: Status::Implemented,
    },
    LoadCase {
        name: "current::list",
        covers: &["Current::list"],
        family: Family::Current,
        status: Status::Implemented,
    },
    // generic/versioned.rs — partition-scoped point-in-time (fast path).
    LoadCase {
        name: "versioned::fetch_one",
        covers: &["Versioned::fetch_one"],
        family: Family::Versioned,
        status: Status::Implemented,
    },
    LoadCase {
        name: "versioned::fetch_optional",
        covers: &["Versioned::fetch_optional"],
        family: Family::Versioned,
        status: Status::Implemented,
    },
    LoadCase {
        name: "versioned::fetch_all_for_partition",
        covers: &["Versioned::fetch_all_for_partition"],
        family: Family::Versioned,
        status: Status::Implemented,
    },
    LoadCase {
        name: "versioned::fetch_all_chunked",
        covers: &["Versioned::fetch_all_chunked"],
        family: Family::Versioned,
        status: Status::Implemented,
    },
    // generic/historic.rs — cross-partition ranked-CTE scan (slow path).
    LoadCase {
        name: "historic::fetch_one",
        covers: &["Historic::fetch_one"],
        family: Family::Historic,
        status: Status::Implemented,
    },
    LoadCase {
        name: "historic::fetch_optional",
        covers: &["Historic::fetch_optional"],
        family: Family::Historic,
        status: Status::Implemented,
    },
    LoadCase {
        name: "historic::fetch_all",
        covers: &["Historic::fetch_all"],
        family: Family::Historic,
        status: Status::Implemented,
    },
    LoadCase {
        name: "historic::fetch_all_chunked",
        covers: &["Historic::fetch_all_chunked"],
        family: Family::Historic,
        status: Status::Implemented,
    },
    LoadCase {
        name: "historic::list_versions",
        covers: &["Historic::list_versions"],
        family: Family::Historic,
        status: Status::Implemented,
    },
    LoadCase {
        name: "historic::list_full",
        covers: &["Historic::list_full"],
        family: Family::Historic,
        status: Status::Implemented,
    },
    // generic/data_version.rs — Versions-selector reads (head/initial/range).
    LoadCase {
        name: "data_version::fetch_one",
        covers: &["VersionedDataVersion::fetch_one"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "data_version::fetch_optional",
        covers: &["VersionedDataVersion::fetch_optional"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "data_version::fetch_all",
        covers: &["VersionedDataVersion::fetch_all"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    LoadCase {
        name: "data_version::fetch_relative_offset",
        covers: &["VersionedDataVersion::fetch_relative_offset"],
        family: Family::DataVersion,
        status: Status::Implemented,
    },
    // generic/recursive.rs — three recursion surfaces over the same walker.
    LoadCase {
        name: "recursive::live",
        covers: &["RecursiveQuery::fetch_all"],
        family: Family::Recursive,
        status: Status::Implemented,
    },
    LoadCase {
        name: "recursive::current",
        covers: &["CurrentRecursive::fetch_all"],
        family: Family::Recursive,
        status: Status::Implemented,
    },
    LoadCase {
        name: "recursive::historic",
        covers: &["HistoricRecursive::fetch_all"],
        family: Family::Recursive,
        status: Status::Implemented,
    },
    // daos/function_runs.rs — logical-chain readiness/commit/dispatch.
    LoadCase {
        name: "lfr::to_execute",
        covers: &["Lfr::to_execute"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::to_commit",
        covers: &["Lfr::to_commit"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::previous_for_function_at",
        covers: &["Lfr::previous_for_function_at"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::for_dispatch",
        covers: &["Lfr::for_dispatch"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::blocked_ids",
        covers: &["Lfr::blocked_ids"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::downstream_one_hop",
        covers: &["Lfr::downstream_one_hop"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::effective_output_base_paths",
        covers: &["Lfr::effective_output_base_paths"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "lfr::with_bundle_base_path",
        covers: &["Lfr::with_bundle_base_path"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pfr::to_dispatch",
        covers: &["Pfr::to_dispatch"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pfr::dispatched_before",
        covers: &["Pfr::dispatched_before"],
        family: Family::Chain,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pfr::in_flight_for",
        covers: &["Pfr::in_flight_for"],
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
    // daos/pointers.rs — pointer event log.
    LoadCase {
        name: "pointers::concrete",
        covers: &["LogicalPointers::concrete"],
        family: Family::Pointer,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pointers::bind_all",
        covers: &["LogicalPointers::bind_all"],
        family: Family::Pointer,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pointers::redirect_replay_attempts",
        covers: &["LogicalPointers::redirect_replay_attempts"],
        family: Family::Pointer,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pointers::redirect_outputs",
        covers: &["LogicalPointers::redirect_outputs"],
        family: Family::Pointer,
        status: Status::Implemented,
    },
    LoadCase {
        name: "pointers::at_concrete",
        covers: &["PointersAt::concrete"],
        family: Family::Pointer,
        status: Status::Implemented,
    },
    // daos/table_data_versions.rs — PTDV-with-names + pointer-bound filter.
    LoadCase {
        name: "ltdv::latest_bound_tdv_with_data_for_table",
        covers: &["Ltdv::latest_bound_tdv_with_data_for_table"],
        family: Family::Pointer,
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
];
