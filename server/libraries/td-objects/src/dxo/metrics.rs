//
// Copyright 2026 Tabsdata Inc.
//

//! Request and response shapes of `/metrics` and `/inflight`.

use crate::dxo::query_filters::{DimensionFilter, TimeWindow, WindowAlign, WindowUnit};
use crate::types::basic::{
    ByteCount, CollectionId, CollectionName, EpochMs, FunctionRunCount, GroupId, GroupName,
    IncludePrivate, IncludeReplaced, LogicalFunctionRunStatus, ProjectId, ProjectName, RowCount,
    TableCount, TableVersionCount, WindowSpan,
};
use std::collections::BTreeMap;
use ta_objects::{TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
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

/// In flight is defined by the run's status, not by `ended_on IS NULL`: a
/// replay clears `ended_on`.
#[ta_objects::Dlo]
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct InflightRow {
    pub project_id: ProjectId,
    pub project_name: Option<ProjectName>,
    pub collection_id: CollectionId,
    pub collection_name: CollectionName,
    pub group_id: GroupId,
    pub group_name: Option<GroupName>,
    pub status: LogicalFunctionRunStatus,
    pub count: i64,
}

#[ta_objects::Dto]
pub struct InflightResponse {
    pub rows: Vec<InflightRow>,
}

#[derive(
    TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash, Copy,
)]
pub enum Dimension {
    Collection,
    Group,
}

#[derive(
    TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq, Ord, PartialOrd, Hash, Copy,
)]
pub enum NodeKind {
    Dim,
    ExternalSource,
    ExternalDestination,
}

/// `*_out` is what the node's runs wrote, `*_in` what they read;
/// `logical_tables_*` are distinct within this status.
#[ta_objects::Dto]
pub struct NodeStatusMeasures {
    pub fn_runs: FunctionRunCount,
    pub table_versions_out: TableVersionCount,
    pub logical_tables_out: TableCount,
    /// Written in the window and not bound as an input by its end.
    pub versions_unconsumed: TableVersionCount,
    pub row_count_out: RowCount,
    pub data_bytes_out: ByteCount,
    pub table_versions_in: TableVersionCount,
    pub logical_tables_in: TableCount,
    pub row_count_in: RowCount,
    pub data_bytes_in: ByteCount,
}

/// The table counts are distinct over every requested status; the additive
/// measures are split by status.
#[ta_objects::Dto]
pub struct NodeRow {
    pub project_id: Option<ProjectId>,
    pub project_name: Option<ProjectName>,
    pub kind: NodeKind,
    pub collection_id: Option<CollectionId>,
    pub collection_name: Option<CollectionName>,
    pub group_id: Option<GroupId>,
    pub group_name: Option<GroupName>,
    pub logical_tables_out: TableCount,
    /// Tables none of whose versions in the window was consumed by its end.
    pub tables_unread: TableCount,
    pub logical_tables_in: TableCount,
    pub by_status: BTreeMap<LogicalFunctionRunStatus, NodeStatusMeasures>,
}

/// `tables` counts versions, `logical_tables` the distinct tables within
/// this status.
#[ta_objects::Dto]
pub struct EdgeStatusMeasures {
    pub fn_runs: FunctionRunCount,
    pub tables: TableVersionCount,
    pub logical_tables: TableCount,
    pub row_count: RowCount,
    pub data_bytes: ByteCount,
}

#[ta_objects::Dto]
pub struct EdgeRow {
    pub project_id: Option<ProjectId>,
    pub project_name: Option<ProjectName>,
    pub source_kind: NodeKind,
    pub source: Option<String>,
    pub source_name: Option<String>,
    pub target_kind: NodeKind,
    pub target: Option<String>,
    pub target_name: Option<String>,
    pub logical_tables: TableCount,
    pub by_status: BTreeMap<LogicalFunctionRunStatus, EdgeStatusMeasures>,
}

#[ta_objects::Dto]
pub struct MetricsResponse {
    pub dim: Dimension,
    pub start_ms: EpochMs,
    pub end_ms: EpochMs,
    pub nodes: Vec<NodeRow>,
    pub edges: Vec<EdgeRow>,
}

/// Filters apply target-only to nodes and either-endpoint to edges.
#[derive(Clone)]
pub struct MetricsRequest {
    pub dim: Dimension,
    pub window: TimeWindow,
    pub collections: DimensionFilter<CollectionId>,
    pub groups: DimensionFilter<GroupId>,
    pub statuses: Vec<LogicalFunctionRunStatus>,
    pub include_replaced: IncludeReplaced,
    pub include_private: IncludePrivate,
}

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

/// Flat fields: `#[serde(flatten)]` cannot deserialize repeated-key `Vec`
/// params under `serde_html_form`.
#[ta_objects::QueryParam]
pub struct MetricsQuery {
    /// Default `collection`.
    #[serde(default)]
    pub dim: Option<Dimension>,
    /// Default `day`.
    #[serde(default)]
    pub unit: Option<WindowUnit>,
    /// `calendar` (default) snaps the window start to the unit boundary
    /// containing `from`; `rolling` ends a `span * unit` window at `from`.
    #[serde(default)]
    pub align: Option<WindowAlign>,
    /// Exclusive right edge, UTC epoch ms. Default now.
    #[serde(default)]
    pub from: Option<EpochMs>,
    /// Window width in units. Default 1.
    #[serde(default)]
    pub span: Option<WindowSpan>,
    #[serde(default, rename = "collection")]
    pub collection: Vec<CollectionId>,
    /// Wins over `collection` on overlap.
    #[serde(default, rename = "collection_exclude")]
    pub collection_exclude: Vec<CollectionId>,
    #[serde(default, rename = "group")]
    pub group: Vec<GroupId>,
    #[serde(default, rename = "group_exclude")]
    pub group_exclude: Vec<GroupId>,
    /// ENDED statuses only; repeat the param for several. Default `Committed`.
    #[serde(default)]
    pub status: Vec<LogicalFunctionRunStatus>,
    /// Count the attempts a replay superseded too. Default false.
    #[serde(default)]
    pub include_replaced: Option<IncludeReplaced>,
    /// Count the versions of private tables too. Default false.
    #[serde(default)]
    pub include_private: Option<IncludePrivate>,
}

#[ta_objects::QueryParam]
pub struct InflightQuery {
    #[serde(default, rename = "collection")]
    pub collection: Vec<CollectionId>,
    #[serde(default, rename = "collection_exclude")]
    pub collection_exclude: Vec<CollectionId>,
    #[serde(default, rename = "group")]
    pub group: Vec<GroupId>,
    #[serde(default, rename = "group_exclude")]
    pub group_exclude: Vec<GroupId>,
    #[serde(default)]
    pub status: Vec<LogicalFunctionRunStatus>,
}

/// The inflight half of `GET /snapshot`, a second extractor beside
/// [`MetricsQuery`].
#[ta_objects::QueryParam]
pub struct InflightStatusQuery {
    /// Default every in-flight status.
    #[serde(default, rename = "inflight_status")]
    pub inflight_status: Vec<LogicalFunctionRunStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn by_status_serializes_keyed_by_status_code() {
        let mut by_status = BTreeMap::new();
        by_status.insert(
            LogicalFunctionRunStatus::Committed,
            EdgeStatusMeasures {
                fn_runs: FunctionRunCount::try_from(1).unwrap(),
                tables: TableVersionCount::try_from(2).unwrap(),
                logical_tables: TableCount::try_from(1).unwrap(),
                row_count: RowCount::try_from(3).unwrap(),
                data_bytes: ByteCount::try_from(4).unwrap(),
            },
        );
        let edge = EdgeRow {
            project_id: None,
            project_name: None,
            source_kind: NodeKind::ExternalSource,
            source: None,
            source_name: None,
            target_kind: NodeKind::Dim,
            target: Some("c".to_string()),
            target_name: None,
            logical_tables: TableCount::try_from(1).unwrap(),
            by_status,
        };
        let json = serde_json::to_value(&edge).unwrap();
        assert_eq!(json["by_status"]["C"]["tables"], 2);
        let back: EdgeRow = serde_json::from_value(json).unwrap();
        assert_eq!(
            *back.by_status[&LogicalFunctionRunStatus::Committed].tables,
            2
        );
    }
}
