//
// Copyright 2026 Tabs Data Inc.
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
        LogicalFunctionRunStatus, RowCount, TableVersionCount,
    };

    /// One row of `/inflight`: count of in-flight runs broken out by
    /// `(collection_id, group_id, status)`. Inflight is defined by status,
    /// not by `ended_on IS NULL` — replays mean the same LFR can lose its
    /// `ended_on` if a new attempt is bound.
    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct InflightRow {
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

    /// One node row per `(bucket, dim_id, group_id, status)` aggregating
    /// LFRs whose target is in this node.
    ///
    /// `*_out` are this node's LFRs as producers (PFR aggregates from the
    /// worker, system-excluded by construction); `*_in` are this node's
    /// LFRs as consumers (sum over source LTDV row_count/data_bytes via
    /// LFRI with `dep_pos >= 0`, system tables excluded explicitly).
    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct NodeRow {
        pub bucket: EpochMs,
        /// Present iff `dim=collection`. NULL when `dim=group`.
        pub collection_id: Option<CollectionId>,
        /// Display name resolved from `collections.name` at query time;
        /// NULL when `dim=group` (no collection axis).
        pub collection_name: Option<CollectionName>,
        /// Snapshot group_id from the LFR's collection at plan-build time.
        /// When `dim=group` this is the dim id; when `dim=collection`
        /// this is metadata (lets the UI bucket nodes into
        /// source/destination/etc. without a separate `/groups` call).
        pub group_id: GroupId,
        /// Display name resolved from `groups.name` at query time.
        pub group_name: Option<GroupName>,
        pub status: LogicalFunctionRunStatus,

        pub fn_runs: i64,
        pub transactions: i64,

        pub table_versions_out: TableVersionCount,
        pub row_count_out: RowCount,
        pub data_bytes_out: ByteCount,

        pub table_versions_in: TableVersionCount,
        pub row_count_in: RowCount,
        pub data_bytes_in: ByteCount,
    }

    /// One edge per `(bucket, source, target, status)` capturing data
    /// flow between dim entities.
    ///
    /// `source` is NULL for synthetic external edges (publisher LFR with
    /// no user-dep inputs). `tables` counts distinct non-system LTDVs
    /// flowing on this edge. `row_count` / `data_bytes` are
    /// source-LTDV-attributed for real edges (input weight that flowed)
    /// and publisher-LFR-attributed for synthetic edges (data the
    /// publisher pushed in from outside Tabsdata). `source` / `target`
    /// are typed by the envelope's `dim`.
    #[ta_objects::Dlo]
    #[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
    pub struct EdgeRow {
        pub bucket: EpochMs,
        pub source: Option<String>,
        /// Display name of the source dim entity; NULL on synthetic
        /// external edges (publisher with no upstream).
        pub source_name: Option<String>,
        pub target: String,
        pub target_name: Option<String>,
        pub status: LogicalFunctionRunStatus,

        pub fn_runs: i64,
        pub tables: i64,
        pub row_count: RowCount,
        pub data_bytes: ByteCount,
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
    /// the dim selection, the dimension filters, and the single status
    /// to aggregate. Filters apply target-only to nodes and
    /// either-endpoint to real edges (target-only to synthetic edges,
    /// since they have no source dim to filter).
    pub struct MetricsRequest {
        pub dim: Dim,
        pub window: TimeWindow,
        pub unit: WindowUnit,
        pub collections: DimensionFilter<CollectionId>,
        pub groups: DimensionFilter<GroupId>,
        pub statuses: Vec<LogicalFunctionRunStatus>,
    }

    /// Domain request for `GET /inflight`. Point-in-time, no window.
    pub struct InflightRequest {
        pub collections: DimensionFilter<CollectionId>,
        pub groups: DimensionFilter<GroupId>,
        pub statuses: Vec<LogicalFunctionRunStatus>,
    }

    /// URL-query view of `MetricsRequest`.
    ///
    /// Window grammar: `unit` (default `day`), `align` (default
    /// `calendar`), `from` (default server-now), `span` (default 1).
    /// `unit` is also the GROUP BY grain — `unit=hour&span=24` returns
    /// 24 hourly rows. `dim` defaults to `collection`.
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
        /// Single status to aggregate. Defaults to `Committed`. Pass
        /// any [`LogicalFunctionRunStatus`] variant; non-ENDED values
        /// (e.g. `Running`) will return no rows since the SQL filters
        /// via `ended_on IS NOT NULL`.
        #[serde(default)]
        #[builder(default)]
        pub status: Option<LogicalFunctionRunStatus>,
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
}

pub use definitions::*;

use crate::types::basic::ProjectId;
use td_tower::project_scope::Scoped;

impl MetricsRequest {
    /// Scope this request to a project, producing the service input.
    pub fn in_project(self, project: impl Into<ProjectId>) -> Scoped<Self, ProjectId> {
        Scoped {
            project: project.into(),
            request: self,
        }
    }
}

impl InflightRequest {
    /// Scope this request to a project, producing the service input.
    pub fn in_project(self, project: impl Into<ProjectId>) -> Scoped<Self, ProjectId> {
        Scoped {
            project: project.into(),
            request: self,
        }
    }
}
