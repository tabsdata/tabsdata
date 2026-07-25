//
// Copyright 2026 Tabsdata Inc.
//

//! Response shapes for the operational `/metrics` endpoint (nodes +
//! edges over a window, parameterised by `dim`) and the point-in-time
//! `/inflight` endpoint.

use td_error::td_error;

#[td_error]
pub enum StatusFilterError {
    #[error("status `{status}` is not ENDED; /metrics aggregates only ENDED statuses ({allowed})")]
    NotEnded { status: String, allowed: String } = 0,
    #[error(
        "status `{status}` is not IN_FLIGHT; /inflight returns only in-flight statuses ({allowed})"
    )]
    NotInFlight { status: String, allowed: String } = 1,
}

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::query_filters::{DimensionFilter, TimeWindow, WindowAlign, WindowUnit};
    use crate::types::basic::{
        ByteCount, CollectionId, CollectionName, EpochMs, GroupId, GroupName,
        LogicalFunctionRunStatus, ProjectId, ProjectName, RowCount, TableId, TableVersionCount,
    };

    /// One row of `/inflight`: count of in-flight runs broken out by
    /// `(collection_id, group_id, status)`. Inflight is defined by status,
    /// not by `ended_on IS NULL` — replays mean the same LFR can lose its
    /// `ended_on` if a new attempt is bound.
    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct InflightRow {
        /// Owning project. Cross-project (global) responses carry rows from
        /// many projects whose collection/group names may collide; the
        /// project distinguishes them. Constant under the tenant-scoped path.
        pub project_id: ProjectId,
        pub project_name: Option<ProjectName>,
        pub collection_id: CollectionId,
        pub collection_name: CollectionName,
        pub group_id: GroupId,
        pub group_name: Option<GroupName>,
        pub status: LogicalFunctionRunStatus,
        pub count: i64,
    }

    /// Response envelope for `GET /inflight`.
    #[ta_objects::Dto]
    pub struct InflightResponse {
        pub rows: Vec<InflightRow>,
    }

    /// Breakout dimension for the `/metrics` response. Selects whether
    /// nodes/edges are keyed on collection_id or group_id.
    #[ta_objects::typed_enum]
    #[derive(Eq, PartialEq, Hash, Copy)]
    pub enum Dim {
        Collection,
        Group,
    }

    #[ta_objects::typed_enum]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Copy)]
    pub enum NodeKind {
        /// Real collection/group node
        Dim,
        /// Synthetic "outside the system" node where data arrives.
        ExternalSource,
        /// Synthetic "outside the system" node where data leaves.
        ExternalDestination,
        /// Synthetic dead end node.
        Unused,
    }

    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct NodeRow {
        pub bucket: EpochMs,
        /// Owning project of a real (`Dim`) node — lets cross-project (global)
        /// responses distinguish same-named collections/groups across projects.
        /// `None` on synthetic rows (external/unused singletons span projects).
        pub project_id: Option<ProjectId>,
        pub project_name: Option<ProjectName>,
        /// `Dim` for real nodes; one of the synthetic variants for
        /// singleton external/unused nodes.
        pub kind: NodeKind,
        /// Present if `kind = Dim` and `dim=collection`. `None` on
        /// synthetic rows and on `dim=group` real rows.
        pub collection_id: Option<CollectionId>,
        /// Display name resolved from `collections.name` at query time;
        /// `None` whenever `collection_id` is `None`.
        pub collection_name: Option<CollectionName>,
        /// Snapshot group_id from the LFR's collection at plan-build time.
        /// When `dim=group` this is the dim id; when `dim=collection`
        /// this is metadata. `None` on synthetic rows.
        pub group_id: Option<GroupId>,
        /// Display name resolved from `groups.name` at query time.
        pub group_name: Option<GroupName>,
        /// Producer-side LFR status for `ExternalSource` / `Unused`;
        /// consumer-side LFR status for `Dim` / `ExternalDestination`.
        /// The endpoint's status filter is the semantic gate — see
        /// the per-branch SQL.
        pub status: LogicalFunctionRunStatus,

        pub fn_runs: i64,
        pub transactions: i64,

        pub table_versions_out: TableVersionCount,
        /// Distinct logical tables output (all versions of the same table folded
        /// into one), across every run folded into this node. Unlike
        /// `table_versions_out` this counts a table even when it only emitted
        /// NoData versions in the window (no `has_data` filter), and it is NOT
        /// summable, so the SQL node query leaves it zero and the service layer
        /// fills it from [`NodeOutputTableRow`] — see
        /// `td_services::metrics::layers::metrics_response`. Always zero on the
        /// synthetic (`ExternalSource` / `ExternalDestination` / `Unused`)
        /// nodes, which are edge-derived and carry no table identity.
        pub logical_tables_out: TableVersionCount,
        pub row_count_out: RowCount,
        pub data_bytes_out: ByteCount,

        pub table_versions_in: TableVersionCount,
        pub row_count_in: RowCount,
        pub data_bytes_in: ByteCount,
    }

    /// One `(node, logical table)` pair among a node's non-system, non-private
    /// output LTDVs — the distinct-table listing that backs
    /// `NodeRow::logical_tables_out`. No `has_data` filter, so a table present
    /// only via NoData versions is included. Returned at the SQL bucket grain
    /// (Month for Quarter/Year); the service layer folds to the requested grain
    /// and counts distinct `table_id` per node, so a table appearing across
    /// several runs — or several months of a quarter/year — is counted once.
    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct NodeOutputTableRow {
        pub bucket: EpochMs,
        pub project_id: Option<ProjectId>,
        pub collection_id: Option<CollectionId>,
        pub group_id: Option<GroupId>,
        pub status: LogicalFunctionRunStatus,
        pub table_id: TableId,
    }

    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct EdgeRow {
        pub bucket: EpochMs,
        /// Owning project of the edge's real dim endpoint(s). Edges never cross
        /// projects (dependencies are project-scoped), so a single project
        /// applies. `None` only if both endpoints are synthetic (does not occur).
        pub project_id: Option<ProjectId>,
        pub project_name: Option<ProjectName>,
        pub source_kind: NodeKind,
        pub source: Option<String>,
        /// Display name of the source dim entity; `None` on synthetic
        /// sources.
        pub source_name: Option<String>,
        pub target_kind: NodeKind,
        pub target: Option<String>,
        /// Display name of the target dim entity; `None` on synthetic
        /// targets.
        pub target_name: Option<String>,
        pub status: LogicalFunctionRunStatus,

        pub fn_runs: i64,
        pub tables: i64,
        /// Distinct logical tables flowing on this edge (all versions of the
        /// same table folded into one), across every run folded into it —
        /// e.g. an edge carrying `orders` v5 and v6 has `tables = 2` but
        /// `logical_tables = 1`. Unlike `tables` this is NOT summable, so the
        /// SQL edge query leaves it zero and the service layer fills it from
        /// [`EdgeTableRow`] — see
        /// `td_services::metrics::layers::metrics_response`.
        pub logical_tables: i64,
        pub row_count: RowCount,
        pub data_bytes: ByteCount,
    }

    /// One `(edge, logical table)` pair among the non-system, non-private
    /// table-versions flowing on an edge — the distinct-table listing that
    /// backs `EdgeRow::logical_tables`. Returned at the SQL bucket grain (Month
    /// for Quarter/Year); the service layer folds to the requested grain and
    /// counts distinct `table_id` per edge, so a table crossing an edge across
    /// several runs — or several months of a quarter/year — is counted once.
    /// The edge identity mirrors [`EdgeRow`]'s fold key
    /// `(bucket, source_kind, source, target_kind, target, status)`;
    /// `project_id` rides along for the cross-project visibility filter.
    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct EdgeTableRow {
        pub bucket: EpochMs,
        pub project_id: Option<ProjectId>,
        pub source_kind: NodeKind,
        pub source: Option<String>,
        pub target_kind: NodeKind,
        pub target: Option<String>,
        pub status: LogicalFunctionRunStatus,
        pub table_id: TableId,
    }

    #[ta_objects::Dto]
    pub struct MetricsResponse {
        pub dim: Dim,
        pub unit: WindowUnit,
        pub start_ms: i64,
        pub end_ms: i64,
        pub nodes: Vec<NodeRow>,
        pub edges: Vec<EdgeRow>,
    }

    /// Domain request for `GET /metrics`. Carries the resolved window,
    /// the dim selection, the dimension filters, and the statuses
    /// to aggregate. Filters apply target-only to nodes and
    /// either-endpoint to real edges (target-only to synthetic edges,
    /// since they have no source dim to filter).
    #[derive(Clone)]
    pub struct MetricsRequest {
        pub dim: Dim,
        pub window: TimeWindow,
        pub unit: WindowUnit,
        pub collections: DimensionFilter<CollectionId>,
        pub groups: DimensionFilter<GroupId>,
        pub statuses: Vec<LogicalFunctionRunStatus>,
    }

    /// Domain request for `GET /inflight`. Point-in-time, no window.
    #[derive(Clone)]
    pub struct InflightRequest {
        pub collections: DimensionFilter<CollectionId>,
        pub groups: DimensionFilter<GroupId>,
        pub statuses: Vec<LogicalFunctionRunStatus>,
    }

    #[derive(Clone)]
    pub struct SnapshotRequest {
        pub metrics: MetricsRequest,
        pub inflight: InflightRequest,
    }

    #[ta_objects::Dto]
    pub struct SnapshotResponse {
        pub metrics: MetricsResponse,
        pub inflight: InflightResponse,
    }

    /// URL-query view of `MetricsRequest`. Window grammar: `unit` (default
    /// `day`, also the GROUP BY grain), `align` (default `calendar`), `from`
    /// (default server-now), `span` (default 1); `dim` defaults to
    /// `collection`. Fields are flat — `#[serde(flatten)]` can't deserialize
    /// the repeated-key `Vec` params under `serde_html_form`.
    #[ta_objects::QueryParam]
    pub struct MetricsQuery {
        /// Breakout dimension (`collection` (default) | `group`).
        #[serde(default)]
        #[builder(default)]
        pub dim: Option<Dim>,
        /// Window unit and GROUP BY grain. Defaults to `day`.
        #[serde(default)]
        #[builder(default)]
        pub unit: Option<WindowUnit>,
        /// Window alignment. `calendar` (default) snaps the start of
        /// the window to the calendar-unit boundary containing `from`
        /// (week/month/quarter/year-to-date); `rolling` produces a
        /// `span*unit`-long window ending exactly at `from`.
        #[serde(default)]
        #[builder(default)]
        pub align: Option<WindowAlign>,
        /// Right edge of the window (UTC epoch ms, exclusive). Omitted =
        /// server-now. To look at a past window, pass a past instant
        /// directly. With `align=rolling` and `from = Tuesday midnight`,
        /// `unit=week` yields Tuesday-to-Tuesday windows.
        #[serde(default)]
        #[builder(default)]
        pub from: Option<i64>,
        /// Window width in `unit`s. Default 1. Per-unit caps apply.
        #[serde(default)]
        #[builder(default)]
        pub span: Option<u32>,
        /// Include filter on collection ids. Applies target-only on
        /// nodes; either-endpoint on real edges; target-only on
        /// synthetic edges.
        #[serde(default, rename = "collection")]
        #[builder(default)]
        pub collection: Vec<CollectionId>,
        /// Exclude filter on collection ids. Exclude wins on overlap.
        #[serde(default, rename = "collection_exclude")]
        #[builder(default)]
        pub collection_exclude: Vec<CollectionId>,
        /// Include filter on group ids. Same semantics as `collection`.
        #[serde(default, rename = "group")]
        #[builder(default)]
        pub group: Vec<GroupId>,
        /// Exclude filter on group ids.
        #[serde(default, rename = "group_exclude")]
        #[builder(default)]
        pub group_exclude: Vec<GroupId>,
        /// Statuses to aggregate. Empty defaults to `[Committed]`.
        /// Repeat the param to pass several (`?status=Done&status=Failed`).
        /// Every value must be ENDED — non-ENDED variants (e.g. `Running`)
        /// are rejected, as the SQL filters via `ended_on IS NOT NULL`.
        #[serde(default)]
        #[builder(default)]
        pub status: Vec<LogicalFunctionRunStatus>,
    }

    /// URL-query view of `InflightRequest`. Point-in-time, no window.
    #[ta_objects::QueryParam]
    pub struct InflightQuery {
        #[serde(default, rename = "collection")]
        #[builder(default)]
        pub collection: Vec<CollectionId>,
        #[serde(default, rename = "collection_exclude")]
        #[builder(default)]
        pub collection_exclude: Vec<CollectionId>,
        #[serde(default, rename = "group")]
        #[builder(default)]
        pub group: Vec<GroupId>,
        #[serde(default, rename = "group_exclude")]
        #[builder(default)]
        pub group_exclude: Vec<GroupId>,
        /// Narrow to specific in-flight statuses.
        #[serde(default)]
        #[builder(default)]
        pub status: Vec<LogicalFunctionRunStatus>,
    }

    /// The inflight-half status filter for `GET /snapshot`, read as a second
    /// `Query` extractor alongside [`MetricsQuery`] so the latter is reused
    /// rather than re-declared.
    #[ta_objects::QueryParam]
    pub struct InflightStatusQuery {
        /// IN_FLIGHT statuses for the inflight half. Empty = all in-flight.
        #[serde(default, rename = "inflight_status")]
        #[builder(default)]
        pub inflight_status: Vec<LogicalFunctionRunStatus>,
    }
}

pub use definitions::*;
