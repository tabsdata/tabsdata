//
// Copyright 2026 Tabsdata Inc.
//

//! Operational metrics: `/metrics` nodes and edges per `(bucket, dim,
//! status)` over a time window, and the point-in-time `/inflight`.
//!
//! `/metrics` is LFR-canonical: one row per logical run, bucketed by the
//! current attempt's `ended_on`, so a replay moves the run's bucket and
//! past-window results are not stable across replays. `/inflight` reads
//! `LFR.status`, the only race-free "right now" signal.
//!
//! Invariants every LTDV/PTDV touch keeps: `function_param_pos >= 0 AND
//! private = FALSE` (system and private tables never surface), input-side
//! `dep_pos >= 0`, and the owning collection and project still resolve in
//! their `*_active` views. Output magnitudes come from the producer's output
//! LTDVs, never from the attempt's worker-aggregated totals.

mod edges;
mod filters;
mod inflight;
mod nodes;
#[cfg(test)]
mod tests;
mod window;

use crate::QueryExecutor;
use crate::daos::metrics::edges::{edge_aggregates_query, edge_tables_query};
use crate::daos::metrics::inflight::inflight_query;
use crate::daos::metrics::nodes::{node_aggregates_query, node_output_tables_query};
use crate::daos::metrics::window::WindowBounds;
use sea_query::{Alias, Expr, Func, Iden, IntoIden, SimpleExpr, Value};
use ta_objects::errors::SqlError;
use td_database::access::SeesProjectRows;
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::logical::{
    FunctionRunDBWithNames, FunctionRunDBWithNamesIden, LogicalFunctionRunDB,
    TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::metrics::{
    EdgeRow, EdgeTableRow, InflightRequest, InflightRow, MetricsRequest, NodeKind,
    NodeOutputTableRow, NodeRow,
};
use td_objects::types::visible_projects::VisibleProjects;

/// Result and intermediate column aliases of the metrics statements.
#[derive(Iden, Clone, Copy)]
enum Col {
    Bucket,
    Kind,
    CollectionId,
    CollectionName,
    GroupId,
    GroupName,
    ProjectId,
    ProjectName,
    Status,
    FnRuns,
    Transactions,
    TableVersionsOut,
    LogicalTablesOut,
    RowCountOut,
    DataBytesOut,
    TableVersionsIn,
    RowCountIn,
    DataBytesIn,
    SourceKind,
    Source,
    SourceName,
    TargetKind,
    Target,
    TargetName,
    LfrId,
    SourceLtdvId,
    SourceRowCount,
    SourceDataBytes,
    TableId,
    Tables,
    LogicalTables,
    RowCount,
    DataBytes,
    TablesIn,
    RowsIn,
    BytesIn,
    TablesOut,
    RowsOut,
    BytesOut,
    Count,
}

/// Table and subquery aliases.
#[derive(Iden, Clone, Copy)]
enum Sq {
    P,
    A,
    C,
    G,
    Pr,
    E,
    Ins,
    Outs,
    SrcC,
    TgtC,
    Proj,
}

pub struct Metrics<E> {
    pub(crate) executor: E,
}

// Every metric aggregates project-partitioned relations, so the executor's
// scope must see project rows: `InProject` for a project's own metrics,
// `AcrossProjects` for the global ones. Query-side narrowing by
// `VisibleProjects` is the caller's job.
impl<E> Metrics<E>
where
    E: QueryExecutor + Domained,
    E::Scope: SeesProjectRows,
{
    /// Per-(collection, group, status) count of in-flight LFRs.
    pub async fn inflight(
        self,
        req: &InflightRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<InflightRow>, SqlError> {
        inflight_query(req, visible_projects)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)
    }

    /// One row per `(bucket, dim id, group id, status)`, with output
    /// magnitudes from the run's output LTDVs and input magnitudes from the
    /// versions it consumed.
    pub async fn metrics_nodes(
        self,
        req: &MetricsRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<NodeRow>, SqlError> {
        let bounds = WindowBounds::try_from(&req.window)?;
        node_aggregates_query(req, &bounds, visible_projects)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)
    }

    /// Real edges (consumed version's dim → run's dim) plus the synthetic
    /// external-source, external-destination and unused edges, all with the
    /// magnitudes of the data flowing on the edge.
    pub async fn metrics_edges(
        self,
        req: &MetricsRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<EdgeRow>, SqlError> {
        let bounds = WindowBounds::try_from(&req.window)?;
        edge_aggregates_query(req, &bounds, visible_projects)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)
    }

    /// Distinct `(node, table_id)` pairs behind `NodeRow::logical_tables_out`;
    /// the service layer folds them to the requested grain.
    pub async fn metrics_node_output_tables(
        self,
        req: &MetricsRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<NodeOutputTableRow>, SqlError> {
        let bounds = WindowBounds::try_from(&req.window)?;
        node_output_tables_query(req, &bounds, visible_projects)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)
    }

    /// Distinct `(edge, table_id)` pairs behind `EdgeRow::logical_tables`;
    /// the service layer folds them to the requested grain.
    pub async fn metrics_edge_tables(
        self,
        req: &MetricsRequest,
        visible_projects: &VisibleProjects,
    ) -> Result<Vec<EdgeTableRow>, SqlError> {
        let bounds = WindowBounds::try_from(&req.window)?;
        edge_tables_query(req, &bounds, visible_projects)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)
    }
}

/// The four metrics statements, for plan inspection in the load suite.
#[cfg(feature = "td-test")]
pub fn metrics_plan_sql(req: &MetricsRequest) -> [(&'static str, String); 4] {
    use sea_query::PostgresQueryBuilder;
    let visible_projects = &VisibleProjects::All;
    let bounds = &WindowBounds::try_from(&req.window).expect("valid window");
    [
        (
            "nodes",
            node_aggregates_query(req, bounds, visible_projects).to_string(PostgresQueryBuilder),
        ),
        (
            "edges",
            edge_aggregates_query(req, bounds, visible_projects).to_string(PostgresQueryBuilder),
        ),
        (
            "node_output_tables",
            node_output_tables_query(req, bounds, visible_projects).to_string(PostgresQueryBuilder),
        ),
        (
            "edge_tables",
            edge_tables_query(req, bounds, visible_projects).to_string(PostgresQueryBuilder),
        ),
    ]
}

fn fr_col<I: IntoIden>(c: I) -> SimpleExpr {
    Expr::col((FunctionRunDBWithNamesIden::Table, c))
}

fn tdv_col<I: IntoIden>(c: I) -> SimpleExpr {
    Expr::col((TableDataVersionDBWithNamesIden::Table, c))
}

fn null_str() -> SimpleExpr {
    Expr::value(Value::String(None))
}

/// The typed enum's serialized form, as the `FromRow` decoder reads it.
fn kind_lit(kind: NodeKind) -> SimpleExpr {
    Expr::value(Value::String(Some(kind.to_string())))
}

fn bigint_zero() -> SimpleExpr {
    Func::cast_as(Expr::val(0i64), Alias::new("BIGINT")).into()
}

/// Postgres widens `SUM(BIGINT)` to NUMERIC, which the BIGINT-typed columns
/// would fail to decode.
fn coalesce_sum<C: sea_query::IntoColumnRef>(col: C) -> SimpleExpr {
    let sum: SimpleExpr = Func::sum(Expr::col(col)).into();
    let cast: SimpleExpr = Func::cast_as(sum, Alias::new("BIGINT")).into();
    Func::coalesce([cast, Expr::val(0i64)]).into()
}
