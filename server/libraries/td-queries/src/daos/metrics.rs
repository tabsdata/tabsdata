//
// Copyright 2026 Tabsdata Inc.
//

//! Operational-metrics queries.
//!
//! Backs the consolidated `/metrics` endpoint (nodes + edges per
//! `(bucket, dim, status)`) and `/inflight` (point-in-time, no window).
//! All queries are pure SELECT statements built via sea_query.
//!
//! The aggregation queries are Postgres-shaped: the RLS qual
//! (`current_setting`) is planner-unestimable, so join order is forced
//! window-first with every correlated probe fenced behind `OFFSET 0`
//! laterals — Θ(window) regardless of history. `/metrics` is not served
//! on other backends.
//!
//! ## `/metrics` semantic: LFR-canonical, currently-bound attempt
//!
//! `/metrics` returns one row per **logical** function run, attributed
//! to the **currently-bound PFR's** `ended_on` and metric columns
//! (projected by the `function_runs__with_names` view via the LTDV
//! pointer). Consequence: a replay rebinds the LFR to a new PFR with a
//! new `ended_on`, so the LFR's bucket moves — **past-window results
//! are not stable across replays.** Suitable for "canonical outcome
//! per logical run" dashboards (success rate, Sankey); NOT suitable
//! for billing / retry-storm visibility, which need per-attempt
//! attribution. If/when that use case lands, add a separate
//! PFR-anchored endpoint rather than reshaping this one.
//!
//! `/inflight` queries LFR.status directly; status is the only
//! race-free "right now" signal (replays can transiently null the
//! bound PFR's `ended_on` during the bind window).
//!
//! ## System/private-table exclusion invariant
//!
//! Anywhere LTDVs/PTDVs are touched we filter
//! `LTDV.function_param_pos >= 0` (mirrors the `system` derivation in
//! the schema view) AND `LTDV.private = FALSE` (snapshot of the output
//! table's privateness, stamped at mint). Input-side joins additionally
//! enforce `LFRI.dep_pos >= 0`, which excludes `td.fn_state__*`
//! plumbing on the consumer side.
//!
//! Output-side magnitudes come from the producer's output LTDVs at
//! query time, NOT from the PFR's worker-aggregated metric columns —
//! those keep the full totals (private included) so a future flag can
//! expose them.
//!
//! ## Soft-deleted collection/project exclusion invariant
//!
//! A run or table version owned by a soft-deleted collection — or by a
//! collection under a soft-deleted project — is filtered out everywhere it
//! surfaces (nodes, edges, IN magnitudes), like private/system tables.
//! `apply_active_entity_filter` gates it on the `collections_active` /
//! `projects_active` views; the two are checked separately because
//! `collections_active` gates only the collection's own deletion.

use crate::QueryExecutor;
use chrono::{DateTime, Utc};
use sea_query::{
    Alias, Expr, ExprTrait, Func, Iden, IntoIden, JoinType, NullOrdering, Order, Query,
    SelectStatement, SimpleExpr, UnionType, Value,
};
use ta_objects::errors::SqlError;
use ta_objects::{CurrentDao, SeaValue};
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::collection::{ActiveCollectionDBIden, CollectionDBIden};
use td_objects::dxo::group::GroupDBIden;
use td_objects::dxo::logical::{
    FunctionRunDBWithNames, FunctionRunDBWithNamesIden, LogicalFunctionRunDB,
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalFunctionRunPointerDB,
    LogicalFunctionRunPointerDBIden, LogicalTableDataVersionDBIden,
    TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::metrics::{
    Dim, EdgeRow, EdgeTableRow, InflightRequest, InflightRow, MetricsRequest, NodeKind,
    NodeOutputTableRow, NodeRow,
};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBIden;
use td_objects::dxo::project::{ActiveProjectDBIden, ProjectDBIden};
use td_objects::dxo::query_filters::{DimensionFilter, WindowUnit};
use td_objects::types::basic::{HasData, LogicalFunctionRunStatus, LogicalPointerStatus, Private};

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
    SourceProjectId,
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

query_handle! {
    /// Entry trait providing [`metrics`](MetricsEntry::metrics) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Metrics`] handle.
    pub trait MetricsEntry {
        fn metrics(self) -> Metrics<Self> {
            Metrics { executor: self }
        }
    }
}

pub struct Metrics<E> {
    pub(crate) executor: E,
}

impl<E> Metrics<E>
where
    E: QueryExecutor,
{
    /// `GET /inflight`: per-(collection, group, status) count of LFRs
    /// whose status is in-flight. Status is the source of truth, not
    /// `ended_on IS NULL` — replays can clear an LFR's bound `ended_on`.
    pub async fn inflight(self, req: &InflightRequest) -> Result<Vec<InflightRow>, SqlError> {
        let rows = inflight_query(req)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<LogicalFunctionRunDB>)?;
        Ok(rows)
    }

    /// Node aggregates for `/metrics`: one row per
    /// `(bucket, dim_id, group_id, status)`. `*_out` magnitudes come
    /// from the per-LFR output-LTDV subquery; `*_in` magnitudes from
    /// the LFRI subquery joined to source-LTDV via the LTDV view.
    /// System/private tables excluded on both sides.
    pub async fn metrics_nodes(self, req: &MetricsRequest) -> Result<Vec<NodeRow>, SqlError> {
        let rows = node_aggregates_query(req)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)?;
        Ok(rows)
    }

    /// Edge aggregates for `/metrics`: real edges (LFRI → source-LTDV
    /// dim) plus synthetic external edges (publisher LFRs with no
    /// qualifying upstream user-dep input). Magnitudes are
    /// source-LTDV-attributed on real edges and
    /// publisher-output-LTDV-attributed on synthetic edges (same
    /// "data flowing on this edge" semantic).
    pub async fn metrics_edges(self, req: &MetricsRequest) -> Result<Vec<EdgeRow>, SqlError> {
        let rows = edge_aggregates_query(req)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)?;
        Ok(rows)
    }

    /// Distinct output-table listing backing `NodeRow::logical_tables_out`:
    /// one row per `(node, table_id)` over producer LFRs' non-system,
    /// non-private output LTDVs (no `has_data` filter). The service layer folds
    /// these to the requested bucket grain and counts distinct tables per node.
    pub async fn metrics_node_output_tables(
        self,
        req: &MetricsRequest,
    ) -> Result<Vec<NodeOutputTableRow>, SqlError> {
        let rows = node_output_tables_query(req)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)?;
        Ok(rows)
    }

    /// Distinct edge-table listing backing `EdgeRow::logical_tables`: one row
    /// per `(edge, table_id)` over the same four-branch union `metrics_edges`
    /// aggregates. The service layer folds these to the requested bucket grain
    /// and counts distinct tables per edge, collapsing versions of one table.
    pub async fn metrics_edge_tables(
        self,
        req: &MetricsRequest,
    ) -> Result<Vec<EdgeTableRow>, SqlError> {
        let rows = edge_tables_query(req)
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<FunctionRunDBWithNames>)?;
        Ok(rows)
    }
}

/// The four metrics statements, for plan inspection in the load suite.
#[cfg(all(postgres, feature = "td-test"))]
pub fn metrics_plan_sql(req: &MetricsRequest) -> [(&'static str, String); 4] {
    use sea_query::PostgresQueryBuilder;
    [
        (
            "nodes",
            node_aggregates_query(req).to_string(PostgresQueryBuilder),
        ),
        (
            "edges",
            edge_aggregates_query(req).to_string(PostgresQueryBuilder),
        ),
        (
            "node_output_tables",
            node_output_tables_query(req).to_string(PostgresQueryBuilder),
        ),
        (
            "edge_tables",
            edge_tables_query(req).to_string(PostgresQueryBuilder),
        ),
    ]
}

/// FROM the windowed attempts (`physical_function_runs (ended_on)` index).
/// Two fenced hops resolve each to its bound run: [`lfr_pointer_probe`] via the
/// pointer reverse index, then [`fr_probe`] by primary key — Θ(window),
/// independent of history. [`fr_probe`] keeps the view's own name so downstream
/// is shape-agnostic.
fn from_windowed_runs(stmt: &mut SelectStatement, req: &MetricsRequest) {
    let ended_on = (
        PhysicalFunctionRunDBIden::Table,
        PhysicalFunctionRunDBIden::EndedOn,
    );
    stmt.from(PhysicalFunctionRunDBIden::Table)
        .and_where(Expr::col(ended_on).gte(ms_to_dt(req.window.start_ms)))
        .and_where(Expr::col(ended_on).lt(ms_to_dt(req.window.end_ms)))
        .join_lateral(
            JoinType::InnerJoin,
            lfr_pointer_probe(),
            LogicalFunctionRunPointerDBIden::Table,
            Expr::val(true),
        )
        .join_lateral(
            JoinType::InnerJoin,
            fr_probe(),
            FunctionRunDBWithNamesIden::Table,
            Expr::val(true),
        );
    apply_active_entity_filter(
        stmt,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ),
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::ProjectId,
        ),
    );
    apply_ended_status_filter(stmt);
}

/// Reverse-index hop: window attempt → currently-bound `logical_function_run_id`
/// via `logical_function_run_pointers_current`. INNER JOIN drops attempts that
/// aren't the current binding; `OFFSET 0` keeps it a per-window-row lookup.
fn lfr_pointer_probe() -> SelectStatement {
    let mut stmt = Query::select();
    stmt.column((Sq::P, LogicalFunctionRunPointerDBIden::LogicalFunctionRunId))
        .from_as(
            Alias::new(<LogicalFunctionRunPointerDB as CurrentDao>::current_table_name()),
            Sq::P,
        )
        .and_where(
            Expr::col((
                Sq::P,
                LogicalFunctionRunPointerDBIden::PhysicalFunctionRunId,
            ))
            .equals((
                PhysicalFunctionRunDBIden::Table,
                PhysicalFunctionRunDBIden::Id,
            )),
        )
        .and_where(
            Expr::col((Sq::P, LogicalFunctionRunPointerDBIden::Status))
                .eq(LogicalPointerStatus::Bound.as_sea_value()),
        )
        .offset(0)
        .take()
}

/// The base LFR keyed by its primary key (resolved by [`lfr_pointer_probe`]).
/// Keying on `id`, not `physical_function_run_id`, lets the planner reach the
/// row by index instead of re-expanding the view per row under RLS's ~1-row
/// estimates. `OFFSET 0` fences the lateral so the window keeps driving; the
/// explicit column list avoids the unpruned target list of a fenced subquery.
/// Ids only — the aggregators attach names after aggregation and take the
/// bucket from the driver PFR. Keeps the view's alias so downstream is
/// shape-agnostic.
fn fr_probe() -> SelectStatement {
    let mut stmt = Query::select();
    for col in [
        LogicalFunctionRunDBIden::Id,
        LogicalFunctionRunDBIden::CollectionId,
        LogicalFunctionRunDBIden::GroupId,
        LogicalFunctionRunDBIden::ProjectId,
        LogicalFunctionRunDBIden::Status,
        LogicalFunctionRunDBIden::TransactionId,
    ] {
        stmt.column((Sq::P, col));
    }
    stmt.from_as(LogicalFunctionRunDBIden::Table, Sq::P)
        .and_where(Expr::col((Sq::P, LogicalFunctionRunDBIden::Id)).equals((
            LogicalFunctionRunPointerDBIden::Table,
            LogicalFunctionRunPointerDBIden::LogicalFunctionRunId,
        )))
        .offset(0)
        .take()
}

/// Fenced probe of the LTDV view keyed by `inner_col = outer`; same
/// rationale as [`fr_probe`]. Ids and magnitudes only — the aggregators
/// attach names once, after aggregation.
fn tdv_probe<I, O>(inner_col: I, outer: O) -> SelectStatement
where
    I: IntoIden + Copy,
    O: sea_query::IntoColumnRef,
{
    let mut stmt = Query::select();
    for col in [
        LogicalTableDataVersionDBIden::Id.into_iden(),
        LogicalTableDataVersionDBIden::TableId.into_iden(),
        LogicalTableDataVersionDBIden::ProjectId.into_iden(),
        LogicalTableDataVersionDBIden::CollectionId.into_iden(),
        LogicalTableDataVersionDBIden::GroupId.into_iden(),
        LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId.into_iden(),
        LogicalTableDataVersionDBIden::FunctionParamPos.into_iden(),
        LogicalTableDataVersionDBIden::Private.into_iden(),
        TableDataVersionDBWithNamesIden::RowCount.into_iden(),
        TableDataVersionDBWithNamesIden::DataBytes.into_iden(),
        TableDataVersionDBWithNamesIden::HasData.into_iden(),
    ] {
        stmt.column((Sq::P, col));
    }
    stmt.from_as(TableDataVersionDBWithNamesIden::Table, Sq::P)
        .and_where(Expr::col((Sq::P, inner_col)).equals(outer))
        .offset(0)
        .take()
}

/// Join the consumed source LTDV, keyed by the input row's requirement id.
fn join_source_tdv(stmt: &mut SelectStatement) {
    stmt.join_lateral(
        JoinType::InnerJoin,
        tdv_probe(
            LogicalTableDataVersionDBIden::Id,
            (
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            ),
        ),
        TableDataVersionDBWithNamesIden::Table,
        Expr::val(true),
    );
}

/// Join the run's own output LTDVs, keyed by producer id.
fn join_produced_tdv(stmt: &mut SelectStatement) {
    stmt.join_lateral(
        JoinType::InnerJoin,
        tdv_probe(
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            (
                FunctionRunDBWithNamesIden::Table,
                FunctionRunDBWithNamesIden::Id,
            ),
        ),
        TableDataVersionDBWithNamesIden::Table,
        Expr::val(true),
    );
}

/// Fenced probe of the consuming inputs for the current window run, keyed by
/// `logical_function_run_id = fr.id`. Same fence rationale as [`fr_probe`]:
/// without it the planner cross-joins the whole LFRI table against the window
/// runs at wide windows (a cartesian re-running the run-resolution probe once
/// per pair); fenced, it stays an index lookup per window row.
fn lfri_probe() -> SelectStatement {
    let mut stmt = Query::select();
    for col in [
        LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        LogicalFunctionRunInputDBIden::RequirementDependencyPos,
    ] {
        stmt.column((Sq::P, col));
    }
    stmt.from_as(LogicalFunctionRunInputDBIden::Table, Sq::P)
        .and_where(
            Expr::col((Sq::P, LogicalFunctionRunInputDBIden::LogicalFunctionRunId)).equals((
                FunctionRunDBWithNamesIden::Table,
                FunctionRunDBWithNamesIden::Id,
            )),
        )
        .offset(0)
        .take()
}

/// Join the run's consumed inputs, keyed by `LFRI.logical_function_run_id =
/// fr.id`, as a fenced lateral probe.
fn join_consumed_lfri(stmt: &mut SelectStatement) {
    stmt.join_lateral(
        JoinType::InnerJoin,
        lfri_probe(),
        LogicalFunctionRunInputDBIden::Table,
        Expr::val(true),
    );
}

// ---------------------------------------------------------------------------
// /metrics — nodes
// ---------------------------------------------------------------------------

/// Per-`(bucket, dim_id, group_id, status)` node aggregator: base-LFR run
/// resolution + per-LFR magnitude laterals → aggregate by ids → attach
/// dim/project names once. Mirrors [`edge_aggregates_query`]. `dim=group`
/// folds collections together (NULL collection id/name, no per-collection
/// grouping).
fn node_aggregates_query(req: &MetricsRequest) -> SelectStatement {
    let mut inner = Query::select();
    from_windowed_runs(&mut inner, req);
    inner.expr_as(bucket_expr(req), Col::Bucket);
    if matches!(req.dim, Dim::Collection) {
        inner.expr_as(
            fr_col(FunctionRunDBWithNamesIden::CollectionId),
            Col::CollectionId,
        );
    }
    inner
        .expr_as(fr_col(FunctionRunDBWithNamesIden::GroupId), Col::GroupId)
        .expr_as(
            fr_col(FunctionRunDBWithNamesIden::ProjectId),
            Col::ProjectId,
        )
        .expr_as(fr_col(FunctionRunDBWithNamesIden::Status), Col::Status)
        .expr_as(Func::count(Expr::val(1)), Col::FnRuns)
        .expr_as(
            Func::count_distinct(fr_col(FunctionRunDBWithNamesIden::TransactionId)),
            Col::Transactions,
        );
    select_io_sums(&mut inner);
    inner
        .join_lateral(
            JoinType::LeftJoin,
            per_lfr_input_aggregates_lateral(),
            Sq::Ins,
            Expr::val(true),
        )
        .join_lateral(
            JoinType::LeftJoin,
            per_lfr_output_aggregates_lateral(),
            Sq::Outs,
            Expr::val(true),
        );
    apply_status_filter(
        &mut inner,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ),
    );
    apply_run_dim_filters(&mut inner, req);
    inner.group_by_col((
        PhysicalFunctionRunDBIden::Table,
        bucket_column_pfr(req.unit),
    ));
    if matches!(req.dim, Dim::Collection) {
        inner.group_by_col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ));
    }
    inner
        .group_by_col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::GroupId,
        ))
        .group_by_col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::ProjectId,
        ))
        .group_by_col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ));

    let mut outer = Query::select();
    outer
        .expr(Expr::col((Sq::A, Col::Bucket)))
        .expr_as(kind_lit(NodeKind::Dim), Col::Kind);
    match req.dim {
        Dim::Collection => {
            outer.expr(Expr::col((Sq::A, Col::CollectionId))).expr_as(
                Expr::col((Sq::C, CollectionDBIden::Name)),
                Col::CollectionName,
            );
        }
        Dim::Group => {
            outer
                .expr_as(null_str(), Col::CollectionId)
                .expr_as(null_str(), Col::CollectionName);
        }
    }
    outer
        .expr(Expr::col((Sq::A, Col::GroupId)))
        .expr_as(Expr::col((Sq::G, GroupDBIden::Name)), Col::GroupName)
        .expr(Expr::col((Sq::A, Col::ProjectId)))
        .expr_as(Expr::col((Sq::Pr, ProjectDBIden::Name)), Col::ProjectName);
    for c in [
        Col::Status,
        Col::FnRuns,
        Col::Transactions,
        Col::TableVersionsOut,
        Col::RowCountOut,
        Col::DataBytesOut,
        Col::TableVersionsIn,
        Col::RowCountIn,
        Col::DataBytesIn,
    ] {
        outer.expr(Expr::col((Sq::A, c)));
    }
    // Distinct-table count: not summable here; zero placeholder, the service
    // layer fills it from `node_output_tables_query`.
    outer.expr_as(
        Func::cast_as(Expr::val(0i64), Alias::new("BIGINT")),
        Col::LogicalTablesOut,
    );
    outer.from_subquery(inner, Sq::A);
    if matches!(req.dim, Dim::Collection) {
        outer.join_as(
            JoinType::LeftJoin,
            CollectionDBIden::Table,
            Sq::C,
            Expr::col((Sq::C, CollectionDBIden::Id)).equals((Sq::A, Col::CollectionId)),
        );
    }
    outer
        .join_as(
            JoinType::LeftJoin,
            GroupDBIden::Table,
            Sq::G,
            Expr::col((Sq::G, GroupDBIden::Id))
                .equals((Sq::A, Col::GroupId))
                .and(Expr::col((Sq::G, Col::ProjectId)).equals((Sq::A, Col::ProjectId))),
        )
        .join_as(
            JoinType::LeftJoin,
            ProjectDBIden::Table,
            Sq::Pr,
            Expr::col((Sq::Pr, ProjectDBIden::Id)).equals((Sq::A, Col::ProjectId)),
        )
        .order_by((Sq::A, Col::Bucket), Order::Desc);
    outer
}

/// Lateral variant of [`per_lfr_input_aggregates_subquery`], correlated on
/// the outer run. Whole-select aggregate: always exactly one row, NULLs
/// absorbed by the outer COALESCE.
fn per_lfr_input_aggregates_lateral() -> SelectStatement {
    let mut stmt = Query::select();
    stmt.expr_as(
        Func::count_distinct(Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        ))),
        Col::TablesIn,
    )
    .expr_as(
        coalesce_sum((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::RowCount,
        )),
        Col::RowsIn,
    )
    .expr_as(
        coalesce_sum((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::DataBytes,
        )),
        Col::BytesIn,
    )
    .from(LogicalFunctionRunInputDBIden::Table)
    .join_lateral(
        JoinType::InnerJoin,
        tdv_probe(
            LogicalTableDataVersionDBIden::Id,
            (
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            ),
        ),
        TableDataVersionDBWithNamesIden::Table,
        Expr::val(true),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
        .equals((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Id,
        )),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementDependencyPos,
        ))
        .gte(0),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        ))
        .is_not_null(),
    );
    apply_user_table_filter(&mut stmt, TableDataVersionDBWithNamesIden::Table);
    filter_source_tdv_active(&mut stmt);
    stmt
}

/// Lateral variant of [`per_lfr_output_aggregates_subquery`], correlated on
/// the outer run; single view entry through the producer index, so no inner
/// fence is needed.
fn per_lfr_output_aggregates_lateral() -> SelectStatement {
    let mut stmt = Query::select();
    stmt.expr_as(Func::count(Expr::val(1)), Col::TablesOut)
        .expr_as(
            coalesce_sum((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::RowCount,
            )),
            Col::RowsOut,
        )
        .expr_as(
            coalesce_sum((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::DataBytes,
            )),
            Col::BytesOut,
        )
        .from(TableDataVersionDBWithNamesIden::Table)
        .and_where(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
            ))
            .equals((
                FunctionRunDBWithNamesIden::Table,
                FunctionRunDBWithNamesIden::Id,
            )),
        )
        .and_where(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::HasData,
            ))
            .eq(HasData::TRUE.as_sea_value()),
        );
    apply_user_table_filter(&mut stmt, TableDataVersionDBWithNamesIden::Table);
    stmt
}

/// Distinct `(node-key, table_id)` listing backing
/// `NodeRow::logical_tables_out`: the producer LFRs' own non-system,
/// non-private output LTDVs, deliberately without the `has_data` filter of
/// [`per_lfr_output_aggregates_lateral`] (a table with only NoData versions
/// still counts). Same window/status/dim scoping as [`node_aggregates_query`];
/// the service layer folds to the requested grain and counts per node.
fn node_output_tables_query(req: &MetricsRequest) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, req);
    join_produced_tdv(&mut stmt);
    stmt.distinct()
        .expr_as(bucket_expr(req), Col::Bucket)
        .expr_as(
            fr_col(FunctionRunDBWithNamesIden::ProjectId),
            Col::ProjectId,
        );
    let collection_id_expr = match req.dim {
        Dim::Collection => fr_col(FunctionRunDBWithNamesIden::CollectionId),
        Dim::Group => null_str(),
    };
    stmt.expr_as(collection_id_expr, Col::CollectionId)
        .expr_as(fr_col(FunctionRunDBWithNamesIden::GroupId), Col::GroupId)
        .expr_as(fr_col(FunctionRunDBWithNamesIden::Status), Col::Status)
        .expr_as(
            tdv_col(LogicalTableDataVersionDBIden::TableId),
            Col::TableId,
        );
    apply_user_table_filter(&mut stmt, TableDataVersionDBWithNamesIden::Table);
    apply_status_filter(
        &mut stmt,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ),
    );
    apply_run_dim_filters(&mut stmt, req);
    stmt
}

// ---------------------------------------------------------------------------
// /metrics — edges (UNION ALL of real + synthetic branches)
// ---------------------------------------------------------------------------

/// Bucket column on `physical_function_runs` (the window driver); the bucket
/// is read straight from the driver rather than from the resolved run.
fn bucket_column_pfr(unit: WindowUnit) -> PhysicalFunctionRunDBIden {
    match unit {
        WindowUnit::Hour => PhysicalFunctionRunDBIden::EndedOnHourMs,
        WindowUnit::Day => PhysicalFunctionRunDBIden::EndedOnDayMs,
        WindowUnit::Week => PhysicalFunctionRunDBIden::EndedOnWeekMs,
        WindowUnit::Month | WindowUnit::Quarter | WindowUnit::Year => {
            PhysicalFunctionRunDBIden::EndedOnMonthMs
        }
    }
}

fn fr_col(c: FunctionRunDBWithNamesIden) -> SimpleExpr {
    Expr::col((FunctionRunDBWithNamesIden::Table, c))
}
fn tdv_col<I: IntoIden>(c: I) -> SimpleExpr {
    Expr::col((TableDataVersionDBWithNamesIden::Table, c))
}
fn null_str() -> SimpleExpr {
    Expr::value(Value::String(None))
}
fn bucket_expr(req: &MetricsRequest) -> SimpleExpr {
    Expr::col((
        PhysicalFunctionRunDBIden::Table,
        bucket_column_pfr(req.unit),
    ))
}

/// Shared id-only projection for the edge branches (names dropped;
/// resolved once after aggregation). `source`/`target` are dim ids or NULL.
/// `source_project` is the source endpoint's owning project — group ids are
/// only unique per project, so the group-dim name join needs it; the target
/// endpoint resolves against the run's own `project_id`.
fn edge_branch_select(
    stmt: &mut SelectStatement,
    req: &MetricsRequest,
    source_kind: NodeKind,
    source: SimpleExpr,
    target_kind: NodeKind,
    target: SimpleExpr,
    source_project: SimpleExpr,
) {
    stmt.expr_as(bucket_expr(req), Col::Bucket)
        .expr_as(kind_lit(source_kind), Col::SourceKind)
        .expr_as(source, Col::Source)
        .expr_as(kind_lit(target_kind), Col::TargetKind)
        .expr_as(target, Col::Target)
        .expr_as(fr_col(FunctionRunDBWithNamesIden::Status), Col::Status)
        .expr_as(fr_col(FunctionRunDBWithNamesIden::Id), Col::LfrId)
        .expr_as(
            tdv_col(LogicalTableDataVersionDBIden::Id),
            Col::SourceLtdvId,
        )
        .expr_as(
            tdv_col(LogicalTableDataVersionDBIden::TableId),
            Col::TableId,
        )
        .expr_as(
            tdv_col(TableDataVersionDBWithNamesIden::RowCount),
            Col::SourceRowCount,
        )
        .expr_as(
            tdv_col(TableDataVersionDBWithNamesIden::DataBytes),
            Col::SourceDataBytes,
        )
        .expr_as(
            fr_col(FunctionRunDBWithNamesIden::ProjectId),
            Col::ProjectId,
        )
        .expr_as(source_project, Col::SourceProjectId);
}

fn lfri_qualified(stmt: &mut SelectStatement) {
    stmt.and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        ))
        .is_not_null(),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementDependencyPos,
        ))
        .gte(0),
    );
}

/// System/private-table exclusion + status filter — the identical tail on
/// every edge branch; the per-branch dim filters differ and stay inline.
fn apply_edge_user_status_filters(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_user_table_filter(stmt, TableDataVersionDBWithNamesIden::Table);
    apply_status_filter(
        stmt,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ),
    );
}

/// The six OUT/IN magnitude sums over the per-LFR LTDV aggregates.
fn select_io_sums(stmt: &mut SelectStatement) {
    for (sq, from, to) in [
        (Sq::Outs, Col::TablesOut, Col::TableVersionsOut),
        (Sq::Outs, Col::RowsOut, Col::RowCountOut),
        (Sq::Outs, Col::BytesOut, Col::DataBytesOut),
        (Sq::Ins, Col::TablesIn, Col::TableVersionsIn),
        (Sq::Ins, Col::RowsIn, Col::RowCountIn),
        (Sq::Ins, Col::BytesIn, Col::DataBytesIn),
    ] {
        stmt.expr_as(coalesce_sum((sq, from)), to);
    }
}

/// Collection + group dim filters on the run's own dims (nodes; edge
/// branches whose only dim endpoint is the run).
fn apply_run_dim_filters(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_dim_filter(
        stmt,
        &req.collections,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ),
    );
    apply_dim_filter(
        stmt,
        &req.groups,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::GroupId,
        ),
    );
}

/// Collection + group dim filters on the LTDV's own dims (edge branches
/// whose only dim endpoint is the table version).
fn apply_tdv_dim_filters(stmt: &mut SelectStatement, req: &MetricsRequest) {
    apply_dim_filter(
        stmt,
        &req.collections,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::CollectionId,
        ),
    );
    apply_dim_filter(
        stmt,
        &req.groups,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::GroupId,
        ),
    );
}

fn edge_real_branch(req: &MetricsRequest) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, req);
    stmt.distinct();
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::Dim,
        tdv_col(source_dim_iden(req.dim)),
        NodeKind::Dim,
        fr_col(target_dim_iden(req.dim)),
        tdv_col(LogicalTableDataVersionDBIden::ProjectId),
    );
    join_consumed_lfri(&mut stmt);
    join_source_tdv(&mut stmt);
    lfri_qualified(&mut stmt);
    filter_source_tdv_active(&mut stmt);
    apply_edge_user_status_filters(&mut stmt, req);
    apply_dim_filter_either_endpoint(
        &mut stmt,
        &req.collections,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::CollectionId,
        ),
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ),
    );
    apply_dim_filter_either_endpoint(
        &mut stmt,
        &req.groups,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::GroupId,
        ),
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::GroupId,
        ),
    );
    stmt
}

fn edge_synth_branch(req: &MetricsRequest) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, req);
    join_produced_tdv(&mut stmt);
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::ExternalSource,
        null_str(),
        NodeKind::Dim,
        fr_col(target_dim_iden(req.dim)),
        null_str(),
    );
    stmt.join(
        JoinType::LeftJoin,
        LogicalFunctionRunInputDBIden::Table,
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
        .equals((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Id,
        ))
        .and(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            ))
            .is_not_null(),
        )
        .and(
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementDependencyPos,
            ))
            .gte(0),
        ),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        ))
        .is_null(),
    );
    apply_edge_user_status_filters(&mut stmt, req);
    apply_run_dim_filters(&mut stmt, req);
    stmt
}

fn edge_egress_branch(req: &MetricsRequest) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, req);
    stmt.distinct();
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::Dim,
        fr_col(target_dim_iden(req.dim)),
        NodeKind::ExternalDestination,
        null_str(),
        fr_col(FunctionRunDBWithNamesIden::ProjectId),
    );
    join_consumed_lfri(&mut stmt);
    join_source_tdv(&mut stmt);
    lfri_qualified(&mut stmt);
    filter_source_tdv_active(&mut stmt);
    stmt.and_where(
        Expr::exists({
            let mut sub = Query::select();
            sub.expr(Expr::val(1))
                .from(LogicalTableDataVersionDBIden::Table)
                .and_where(
                    Expr::col((
                        LogicalTableDataVersionDBIden::Table,
                        LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
                    ))
                    .equals((
                        FunctionRunDBWithNamesIden::Table,
                        FunctionRunDBWithNamesIden::Id,
                    )),
                );
            apply_user_table_filter(&mut sub, LogicalTableDataVersionDBIden::Table);
            sub
        })
        .not(),
    );
    apply_edge_user_status_filters(&mut stmt, req);
    apply_run_dim_filters(&mut stmt, req);
    stmt
}

fn edge_unused_branch(req: &MetricsRequest) -> SelectStatement {
    let mut stmt = Query::select();
    from_windowed_runs(&mut stmt, req);
    join_produced_tdv(&mut stmt);
    edge_branch_select(
        &mut stmt,
        req,
        NodeKind::Dim,
        tdv_col(source_dim_iden(req.dim)),
        NodeKind::Unused,
        null_str(),
        tdv_col(LogicalTableDataVersionDBIden::ProjectId),
    );
    stmt.and_where(
        Expr::exists({
            let mut sub = Query::select();
            sub.expr(Expr::val(1))
                .from(LogicalFunctionRunInputDBIden::Table)
                .and_where(
                    Expr::col((
                        LogicalFunctionRunInputDBIden::Table,
                        LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
                    ))
                    .equals((
                        TableDataVersionDBWithNamesIden::Table,
                        LogicalTableDataVersionDBIden::Id,
                    )),
                )
                .and_where(
                    Expr::col((
                        LogicalFunctionRunInputDBIden::Table,
                        LogicalFunctionRunInputDBIden::RequirementDependencyPos,
                    ))
                    .gte(0),
                );
            sub
        })
        .not(),
    );
    apply_edge_user_status_filters(&mut stmt, req);
    apply_tdv_dim_filters(&mut stmt, req);
    stmt
}

/// The four-branch edge union (real + synthetic-source + egress + unused),
/// shared by [`edge_aggregates_query`] (aggregates it) and
/// [`edge_tables_query`] (distinct-lists it).
fn edge_inner_union(req: &MetricsRequest) -> SelectStatement {
    let mut inner = edge_real_branch(req);
    inner.union(UnionType::All, edge_synth_branch(req));
    inner.union(UnionType::All, edge_egress_branch(req));
    inner.union(UnionType::All, edge_unused_branch(req));
    inner
}

/// Edge aggregator: id-only branches → aggregate by ids → attach
/// source/target/project names once, keeping per-window-row name joins out
/// of the hot path (measured ~4× faster). Group ids are only unique per
/// project, so the group-dim name joins key on `(project, id)` — the source
/// endpoint's project rides along as `source_project_id`.
fn edge_aggregates_query(req: &MetricsRequest) -> SelectStatement {
    let (dim_table, dim_id, dim_name) = match req.dim {
        Dim::Collection => (
            CollectionDBIden::Table.into_iden(),
            CollectionDBIden::Id.into_iden(),
            CollectionDBIden::Name.into_iden(),
        ),
        Dim::Group => (
            GroupDBIden::Table.into_iden(),
            GroupDBIden::Id.into_iden(),
            GroupDBIden::Name.into_iden(),
        ),
    };

    let inner = edge_inner_union(req);

    let cols = [
        Col::Bucket,
        Col::SourceKind,
        Col::Source,
        Col::TargetKind,
        Col::Target,
        Col::Status,
        Col::ProjectId,
        Col::SourceProjectId,
    ];
    let mut agg = Query::select();
    for c in cols {
        agg.expr(Expr::col((Sq::E, c)));
    }
    agg.expr_as(
        Func::count_distinct(Expr::col((Sq::E, Col::LfrId))),
        Col::FnRuns,
    )
    .expr_as(
        Func::count_distinct(Expr::col((Sq::E, Col::SourceLtdvId))),
        Col::Tables,
    )
    .expr_as(coalesce_sum((Sq::E, Col::SourceRowCount)), Col::RowCount)
    .expr_as(coalesce_sum((Sq::E, Col::SourceDataBytes)), Col::DataBytes)
    .from_subquery(inner, Sq::E);
    for c in cols {
        agg.group_by_col((Sq::E, c));
    }

    let mut outer = Query::select();
    outer
        .expr(Expr::col((Sq::A, Col::Bucket)))
        .expr(Expr::col((Sq::A, Col::SourceKind)))
        .expr(Expr::col((Sq::A, Col::Source)))
        .expr_as(Expr::col((Sq::SrcC, dim_name.clone())), Col::SourceName)
        .expr(Expr::col((Sq::A, Col::TargetKind)))
        .expr(Expr::col((Sq::A, Col::Target)))
        .expr_as(Expr::col((Sq::TgtC, dim_name)), Col::TargetName)
        .expr(Expr::col((Sq::A, Col::Status)))
        .expr(Expr::col((Sq::A, Col::ProjectId)))
        .expr_as(Expr::col((Sq::Proj, ProjectDBIden::Name)), Col::ProjectName)
        .expr(Expr::col((Sq::A, Col::FnRuns)))
        .expr(Expr::col((Sq::A, Col::Tables)))
        // Distinct-table count: not summable here; zero placeholder, the
        // service layer fills it from `edge_tables_query`.
        .expr_as(
            Func::cast_as(Expr::val(0i64), Alias::new("BIGINT")),
            Col::LogicalTables,
        )
        .expr(Expr::col((Sq::A, Col::RowCount)))
        .expr(Expr::col((Sq::A, Col::DataBytes)))
        .from_subquery(agg, Sq::A)
        .join_as(JoinType::LeftJoin, dim_table.clone(), Sq::SrcC, {
            let on = Expr::col((Sq::SrcC, dim_id.clone())).equals((Sq::A, Col::Source));
            match req.dim {
                Dim::Collection => on,
                Dim::Group => on.and(
                    Expr::col((Sq::SrcC, Col::ProjectId)).equals((Sq::A, Col::SourceProjectId)),
                ),
            }
        })
        .join_as(JoinType::LeftJoin, dim_table, Sq::TgtC, {
            let on = Expr::col((Sq::TgtC, dim_id)).equals((Sq::A, Col::Target));
            match req.dim {
                Dim::Collection => on,
                Dim::Group => {
                    on.and(Expr::col((Sq::TgtC, Col::ProjectId)).equals((Sq::A, Col::ProjectId)))
                }
            }
        })
        .join_as(
            JoinType::LeftJoin,
            ProjectDBIden::Table,
            Sq::Proj,
            Expr::col((Sq::Proj, ProjectDBIden::Id)).equals((Sq::A, Col::ProjectId)),
        )
        .order_by((Sq::A, Col::Bucket), Order::Desc)
        .order_by_with_nulls((Sq::A, Col::Source), Order::Asc, NullOrdering::Last)
        .order_by_with_nulls((Sq::A, Col::Target), Order::Asc, NullOrdering::Last)
        .order_by((Sq::A, Col::Status), Order::Asc);
    outer
}

/// Distinct `(edge-key, table_id)` listing backing `EdgeRow::logical_tables`:
/// [`edge_inner_union`] under `SELECT DISTINCT`, names omitted (not part of
/// the edge key). The service layer folds to the requested grain and counts
/// distinct tables per edge.
fn edge_tables_query(req: &MetricsRequest) -> SelectStatement {
    let inner = edge_inner_union(req);
    let mut outer = Query::select();
    outer.distinct();
    for c in [
        Col::Bucket,
        Col::ProjectId,
        Col::SourceKind,
        Col::Source,
        Col::TargetKind,
        Col::Target,
        Col::Status,
        Col::TableId,
    ] {
        outer.expr(Expr::col((Sq::E, c)));
    }
    outer.from_subquery(inner, Sq::E);
    outer
}

// ---------------------------------------------------------------------------
// /inflight
// ---------------------------------------------------------------------------

/// Reads from the bare LFR table with ad-hoc joins to collections and
/// groups for name display — the full `function_runs__with_names` view
/// drags in Functions/Executions/Users joins and a correlated PFR
/// pointer subquery this endpoint doesn't need.
fn inflight_query(req: &InflightRequest) -> SelectStatement {
    let mut stmt = Query::select();
    stmt.column((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::CollectionId,
    ))
    .expr_as(
        Expr::col((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Name)),
        Col::CollectionName,
    )
    .column((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::GroupId,
    ))
    .expr_as(
        Expr::col((GroupDBIden::Table, GroupDBIden::Name)),
        Col::GroupName,
    )
    .column((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::ProjectId,
    ))
    .expr_as(
        Expr::col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Name)),
        Col::ProjectName,
    )
    .column((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::Status,
    ))
    .expr_as(Func::count(Expr::val(1)), Col::Count)
    .from(LogicalFunctionRunDBIden::Table)
    // INNER JOIN drops in-flight runs of a soft-deleted collection; ids are
    // globally unique, so no project qualifier is needed.
    .join(
        JoinType::InnerJoin,
        ActiveCollectionDBIden::Table,
        Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ))
        .equals((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Id)),
    )
    .join(
        JoinType::LeftJoin,
        GroupDBIden::Table,
        // Group ids are only unique per project (the system groups share the
        // same well-known id in every project); matching project_id too keeps
        // this exact under the Global (RLS-bypass) variant — without it the
        // join fans out across projects and inflates the counts.
        Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::GroupId,
        ))
        .equals((GroupDBIden::Table, GroupDBIden::Id))
        .and(
            Expr::col((
                LogicalFunctionRunDBIden::Table,
                LogicalFunctionRunDBIden::ProjectId,
            ))
            // `GroupDB` is a field-less DAO (RLS owns project_id), so the
            // column isn't on its Iden — address it by name.
            .equals((GroupDBIden::Table, Col::ProjectId)),
        ),
    )
    // INNER JOIN drops in-flight runs of a soft-deleted project (the Global
    // cross-project path; the scoped path already resolves to a live project).
    .join(
        JoinType::InnerJoin,
        ActiveProjectDBIden::Table,
        Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::ProjectId,
        ))
        .equals((ActiveProjectDBIden::Table, ActiveProjectDBIden::Id)),
    )
    .and_where(
        Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::Status,
        ))
        .is_in(
            LogicalFunctionRunStatus::IN_FLIGHT
                .iter()
                .map(SeaValue::as_sea_value),
        ),
    )
    .group_by_col((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::CollectionId,
    ))
    .group_by_col((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Name))
    .group_by_col((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::GroupId,
    ))
    .group_by_col((GroupDBIden::Table, GroupDBIden::Name))
    .group_by_col((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::ProjectId,
    ))
    .group_by_col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Name))
    .group_by_col((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::Status,
    ));

    apply_dim_filter(
        &mut stmt,
        &req.collections,
        (
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ),
    );
    apply_dim_filter(
        &mut stmt,
        &req.groups,
        (
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::GroupId,
        ),
    );
    apply_status_filter(
        &mut stmt,
        &req.statuses,
        (
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::Status,
        ),
    );

    stmt
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Per-dim target column on the LFR view.
fn target_dim_iden(dim: Dim) -> FunctionRunDBWithNamesIden {
    match dim {
        Dim::Collection => FunctionRunDBWithNamesIden::CollectionId,
        Dim::Group => FunctionRunDBWithNamesIden::GroupId,
    }
}

/// Per-dim source column on the LTDV-with-names view.
fn source_dim_iden(dim: Dim) -> LogicalTableDataVersionDBIden {
    match dim {
        Dim::Collection => LogicalTableDataVersionDBIden::CollectionId,
        Dim::Group => LogicalTableDataVersionDBIden::GroupId,
    }
}

/// Convert UTC epoch ms to `DateTime<Utc>` for binding against the
/// `ended_on` TIMESTAMPTZ column. Anchor-aligned windows can have
/// sub-bucket bounds, so we filter on the raw timestamp and only use
/// the bucket-floored column for `GROUP BY`.
fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).expect("window bound must be a valid UTC timestamp")
}

/// `status IN ENDED` on the view. Redundant with the window in steady
/// state, but closes the corner case where the bound PFR (and its
/// `ended_on`) is gone while the LFR's sticky status is still ENDED.
fn apply_ended_status_filter(stmt: &mut SelectStatement) {
    let status = (
        FunctionRunDBWithNamesIden::Table,
        FunctionRunDBWithNamesIden::Status,
    );
    let ended_vals: Vec<_> = LogicalFunctionRunStatus::ENDED
        .iter()
        .map(SeaValue::as_sea_value)
        .collect();
    stmt.and_where(Expr::col(status).is_in(ended_vals));
}

/// Project a `NodeKind` as a string literal so the FromRow decoder
/// (which decodes through `String`) sees the typed-enum's serialized
/// form. Used to tag each UNION branch with its synthetic identity.
fn kind_lit(kind: NodeKind) -> SimpleExpr {
    Expr::value(Value::String(Some(kind.to_string())))
}

fn coalesce_sum<C: sea_query::IntoColumnRef + Clone>(col: C) -> SimpleExpr {
    // CAST around the SUM because Postgres widens SUM(BIGINT) to NUMERIC;
    // the DAO column is BIGINT-typed and would fail to decode otherwise.
    let sum: SimpleExpr = Func::sum(Expr::col(col)).into();
    let cast: SimpleExpr = Func::cast_as(sum, Alias::new("BIGINT")).into();
    let zero: SimpleExpr = Expr::val(0i64);
    Func::coalesce([cast, zero]).into()
}

/// System- and private-table exclusion on an LTDV-shaped table/view:
/// `function_param_pos >= 0 AND private = FALSE`. See the module-level
/// invariant note.
fn apply_user_table_filter<T>(stmt: &mut SelectStatement, table: T)
where
    T: IntoIden,
{
    let table = table.into_iden();
    stmt.and_where(
        Expr::col((
            table.clone(),
            LogicalTableDataVersionDBIden::FunctionParamPos,
        ))
        .gte(0),
    )
    .and_where(
        Expr::col((table, LogicalTableDataVersionDBIden::Private))
            .eq(Private::FALSE.as_sea_value()),
    );
}

/// Require `collection_id` / `project_id` to still resolve in the
/// `collections_active` / `projects_active` views — dropping anything owned by
/// a soft-deleted collection or project. Ids are globally unique, so each probe
/// keys on id alone.
fn apply_active_entity_filter<C, P>(stmt: &mut SelectStatement, collection_id: C, project_id: P)
where
    C: sea_query::IntoColumnRef,
    P: sea_query::IntoColumnRef,
{
    stmt.and_where(Expr::exists({
        let mut sub = Query::select();
        sub.expr(Expr::val(1))
            .from(ActiveCollectionDBIden::Table)
            .and_where(
                Expr::col((ActiveCollectionDBIden::Table, ActiveCollectionDBIden::Id))
                    .equals(collection_id),
            );
        sub
    }))
    .and_where(Expr::exists({
        let mut sub = Query::select();
        sub.expr(Expr::val(1))
            .from(ActiveProjectDBIden::Table)
            .and_where(
                Expr::col((ActiveProjectDBIden::Table, ActiveProjectDBIden::Id)).equals(project_id),
            );
        sub
    }));
}

/// [`apply_active_entity_filter`] on a consumed source LTDV alias.
fn filter_source_tdv_active(stmt: &mut SelectStatement) {
    apply_active_entity_filter(
        stmt,
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::CollectionId,
        ),
        (
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::ProjectId,
        ),
    );
}

fn apply_dim_filter<Id, C>(stmt: &mut SelectStatement, filter: &DimensionFilter<Id>, col: C)
where
    Id: SeaValue,
    C: sea_query::IntoColumnRef + Clone,
{
    if !filter.include.is_empty() {
        let vals: Vec<_> = filter.include.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(Expr::col(col.clone()).is_in(vals));
    }
    if !filter.exclude.is_empty() {
        let vals: Vec<_> = filter.exclude.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(Expr::col(col).is_not_in(vals));
    }
}

/// Real-edges filter: edge survives if EITHER endpoint passes include
/// AND BOTH endpoints clear exclude.
fn apply_dim_filter_either_endpoint<Id, C1, C2>(
    stmt: &mut SelectStatement,
    filter: &DimensionFilter<Id>,
    source_col: C1,
    target_col: C2,
) where
    Id: SeaValue,
    C1: sea_query::IntoColumnRef + Clone,
    C2: sea_query::IntoColumnRef + Clone,
{
    if !filter.include.is_empty() {
        let vals_src: Vec<_> = filter.include.iter().map(SeaValue::as_sea_value).collect();
        let vals_tgt: Vec<_> = filter.include.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(
            Expr::col(source_col.clone())
                .is_in(vals_src)
                .or(Expr::col(target_col.clone()).is_in(vals_tgt)),
        );
    }
    if !filter.exclude.is_empty() {
        let vals_src: Vec<_> = filter.exclude.iter().map(SeaValue::as_sea_value).collect();
        let vals_tgt: Vec<_> = filter.exclude.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(Expr::col(source_col).is_not_in(vals_src));
        stmt.and_where(Expr::col(target_col).is_not_in(vals_tgt));
    }
}

fn apply_status_filter<C>(stmt: &mut SelectStatement, statuses: &[LogicalFunctionRunStatus], col: C)
where
    C: sea_query::IntoColumnRef,
{
    if !statuses.is_empty() {
        let vals: Vec<_> = statuses.iter().map(SeaValue::as_sea_value).collect();
        stmt.and_where(Expr::col(col).is_in(vals));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::CurrentEntry;
    use crate::LiveEntry;
    use crate::test_utils::SeedEntry;
    use ta_objects::Buildable;
    use ta_objects::and;
    use td_common::datetime::Timestamp;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDeleteDB};
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::logical::{
        LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalPointerDao,
        LogicalTableDataVersionDB, LogicalTableDataVersionPointerDB,
    };
    use td_objects::dxo::metrics::{Dim, MetricsRequest, NodeKind};
    use td_objects::dxo::physical_function_run::{
        PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB,
    };
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::project::{ActiveProjectDB, ProjectDeleteDB};
    use td_objects::dxo::query_filters::DimensionFilter;
    use td_objects::dxo::query_filters::{TimeWindow, WindowUnit};
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AtTime, BundleId, ByteCount, Decorator, DependencyPos, EpochMs, HasData, InputIdx,
        LogicalFunctionRunStatus, LogicalPointerReason, LogicalPointerStatus,
        PhysicalFunctionRunStatus, RowCount, TableFunctionParamPos, TableNameDto,
        TableVersionCount, TransactionKey, VersionPos,
    };
    use td_objects::types::basic::{CollectionName, ProjectId, ProjectName, UserId};
    use td_security::ENCODED_ID_SYSTEM;

    const WINDOW_DAY_START: i64 = 1_700_000_000_000; // arbitrary day in Nov 2023
    const WINDOW_DAY_END: i64 = WINDOW_DAY_START + 86_400_000;
    const IN_WINDOW: i64 = WINDOW_DAY_START + 3_600_000;

    // ----- Helpers ------------------------------------------------------

    fn day_window() -> TimeWindow {
        TimeWindow::new(WINDOW_DAY_START, WINDOW_DAY_END)
    }

    fn empty_request(dim: Dim) -> MetricsRequest {
        MetricsRequest {
            dim,
            window: day_window(),
            unit: WindowUnit::Day,
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        }
    }

    async fn make_publisher_function(
        db: &TdPool,
        collection: &td_objects::dxo::collection::ActiveCollectionDB,
        name: &str,
    ) -> Result<FunctionDB, TdError> {
        make_publisher_function_with_tables(db, collection, name, &["out"]).await
    }

    async fn make_publisher_function_with_tables(
        db: &TdPool,
        collection: &td_objects::dxo::collection::ActiveCollectionDB,
        name: &str,
        tables: &[&str],
    ) -> Result<FunctionDB, TdError> {
        let tables = tables
            .iter()
            .map(|t| TableNameDto::try_from(*t))
            .collect::<Result<Vec<_>, _>>()?;
        let create = FunctionRegister::builder()
            .try_name(name)?
            .try_description("test fn")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("rv")?
            .reuse_frozen_tables(false)
            .build()?;
        db.scoped(collection.project_id)
            .seed()
            .function(collection, &create)
            .await
    }

    async fn stamp_buckets_on_pfr(db: &TdPool, pfr: &PhysicalFunctionRunDB) -> Result<(), TdError> {
        stamp_buckets_and_metrics_on_pfr(db, pfr, 1, 10, 100).await
    }

    /// The PFR metric columns store the worker's FULL per-attempt
    /// totals (private tables included); /metrics excludes private
    /// tables query-side, so tests stamp totals that differ from the
    /// expected response when private outputs are in play.
    async fn stamp_buckets_and_metrics_on_pfr(
        db: &TdPool,
        pfr: &PhysicalFunctionRunDB,
        tables: i64,
        rows: i64,
        bytes: i64,
    ) -> Result<(), TdError> {
        let bucket = EpochMs::try_from(IN_WINDOW)?;
        let ended_on = AtTime::try_from(Timestamp::try_from(IN_WINDOW)?)?;
        let update = UpdatePhysicalFunctionRunDB::builder()
            .status(PhysicalFunctionRunStatus::Done)
            .ended_on(Some(ended_on))
            .ended_on_hour_ms(Some(bucket))
            .ended_on_day_ms(Some(bucket))
            .ended_on_week_ms(Some(bucket))
            .ended_on_month_ms(Some(bucket))
            .metric_table_versions(Some(TableVersionCount::try_from(tables)?))
            .metric_rows(Some(RowCount::try_from(rows)?))
            .metric_data_bytes(Some(ByteCount::try_from(bytes)?))
            .build()?;
        db.scoped(pfr.project_id)
            .live()
            .update::<_, PhysicalFunctionRunDB>(&update, &pfr.id)
            .await?;
        Ok(())
    }

    /// Bind a PTDV to an existing (Pending) LTDV with the given
    /// `row_count` / `data_bytes`. Simulates dispatch + worker
    /// callback in production: mints a PTDV linked to the LFR's PFR,
    /// appends a Bound pointer event so the view resolves
    /// `latest_bound` to this PTDV, and stamps the magnitude columns.
    async fn bind_ptdv_with_magnitudes(
        db: &TdPool,
        ltdv: &LogicalTableDataVersionDB,
        table: &TableDB,
        pfr: &PhysicalFunctionRunDB,
        row_count: i64,
        data_bytes: i64,
    ) -> Result<PhysicalTableDataVersionDB, TdError> {
        let ptdv = PhysicalTableDataVersionDB::builder()
            .project_id(pfr.project_id)
            .collection_id(ltdv.collection_id)
            .table_id(table.table_id)
            .name(table.name.clone())
            .table_version_id(table.id)
            .function_version_id(&table.function_version_id)
            .has_data(Some(HasData::TRUE))
            .row_count(Some(RowCount::try_from(row_count)?))
            .data_bytes(Some(ByteCount::try_from(data_bytes)?))
            .execution_id(&ltdv.execution_id)
            .transaction_id(&ltdv.transaction_id)
            .physical_function_run_id(&pfr.id)
            .function_param_pos(
                table
                    .function_param_pos
                    .unwrap_or(TableFunctionParamPos::try_from(0)?),
            )
            .triggered_on(pfr.triggered_on)
            .build()?;
        db.scoped(pfr.project_id)
            .live()
            .insert::<PhysicalTableDataVersionDB>(&ptdv)
            .await?;
        let pointer = LogicalTableDataVersionPointerDB::build_event(
            ltdv.project_id,
            &ltdv.id,
            LogicalPointerStatus::Bound,
            Some(ptdv.id.clone()),
            AtTime::try_from(pfr.triggered_on)?,
            ltdv.triggered_by_id,
            LogicalPointerReason::InitialDispatch,
        )?;
        db.scoped(ltdv.project_id)
            .live()
            .insert::<LogicalTableDataVersionPointerDB>(&pointer)
            .await?;
        Ok(ptdv)
    }

    async fn seed_collection(
        db: &TdPool,
        project_id: ProjectId,
        name: &str,
    ) -> Result<ActiveCollectionDB, TdError> {
        db.scoped(project_id)
            .seed()
            .collection(
                &CollectionName::try_from(name)?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await
    }

    async fn soft_delete_collection(db: &TdPool, coll: &ActiveCollectionDB) -> Result<(), TdError> {
        let del = CollectionDeleteDB::builder()
            .name(CollectionName::try_from(format!("deleted_{}", coll.id))?)
            .modified_on(AtTime::try_from(Timestamp::try_from(IN_WINDOW)?)?)
            .modified_by_id(UserId::try_from(ENCODED_ID_SYSTEM)?)
            .name_when_deleted(coll.name.clone())
            .build()?;
        db.scoped(coll.project_id)
            .live()
            .update::<_, ActiveCollectionDB>(&del, &coll.id)
            .await?;
        Ok(())
    }

    async fn soft_delete_project(db: &TdPool, project: &ActiveProjectDB) -> Result<(), TdError> {
        let del = ProjectDeleteDB::builder()
            .name(ProjectName::try_from(format!("deleted_{}", project.id))?)
            .modified_on(AtTime::try_from(Timestamp::try_from(IN_WINDOW)?)?)
            .modified_by_id(UserId::try_from(ENCODED_ID_SYSTEM)?)
            .name_when_deleted(project.name.clone())
            .build()?;
        db.scoped(project.id)
            .live()
            .update::<_, ActiveProjectDB>(&del, &project.id)
            .await?;
        Ok(())
    }

    /// Seed a publisher (no inputs) with a terminated PFR, stamped
    /// metrics, and a bound PTDV carrying row_count + data_bytes. The
    /// LTDV's view-resolved magnitudes will reflect the PTDV.
    async fn seed_publisher_with_ltdv(
        db: &TdPool,
        project_id: ProjectId,
        coll_name: &str,
        fn_name: &str,
    ) -> Result<
        (
            ActiveCollectionDB,
            LogicalFunctionRunDB,
            LogicalTableDataVersionDB,
        ),
        TdError,
    > {
        let coll = seed_collection(db, project_id, coll_name).await?;
        let func = make_publisher_function(db, &coll, fn_name).await?;
        let exec = db.scoped(project_id).seed().execution(&func).await?;
        let trx = db
            .scoped(project_id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(project_id)
            .seed()
            .physical_function_run(&coll, &func, &exec, &trx, &LogicalFunctionRunStatus::Done)
            .await?;
        stamp_buckets_on_pfr(db, &pfr).await?;
        let lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one(and!(&pfr.execution_id, &pfr.function_id))
            .await?;
        let ltdvs: Vec<LogicalTableDataVersionDB> = db.live().fetch_all(&lfr.execution_id).await?;
        let ltdv = ltdvs
            .into_iter()
            .find(|t| t.producer_logical_function_run_id.as_ref() == Some(&lfr.id))
            .expect("publisher mints exactly one output LTDV");

        // Bind a PTDV with the standard test magnitudes (10 / 100), so
        // the LTDV-with-names view resolves to non-NULL row_count and
        // data_bytes.
        let tables: Vec<TableDB> = db.current().fetch_all::<TableDB>(&func.id).await?;
        let table = tables
            .iter()
            .find(|t| t.table_id == ltdv.table_id)
            .expect("LTDV's table_id must match an output table of the function");
        bind_ptdv_with_magnitudes(db, &ltdv, table, &pfr, 10, 100).await?;

        Ok((coll, lfr, ltdv))
    }

    /// Seed an execution/transaction/PFR (Done) for the function and
    /// return the LFR + PFR. Buckets/metrics are NOT stamped — callers
    /// pick the stamp helper.
    async fn seed_done_run(
        db: &TdPool,
        coll: &ActiveCollectionDB,
        func: &FunctionDB,
    ) -> Result<(LogicalFunctionRunDB, PhysicalFunctionRunDB), TdError> {
        let exec = db.scoped(coll.project_id).seed().execution(func).await?;
        let trx = db
            .scoped(coll.project_id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
            .scoped(coll.project_id)
            .seed()
            .physical_function_run(coll, func, &exec, &trx, &LogicalFunctionRunStatus::Done)
            .await?;
        let lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one(and!(&pfr.execution_id, &pfr.function_id))
            .await?;
        Ok((lfr, pfr))
    }

    /// Bind every output LTDV of the run to a PTDV: regular tables get
    /// (10, 100), private tables (7, 77).
    async fn bind_all_outputs(
        db: &TdPool,
        func: &FunctionDB,
        lfr: &LogicalFunctionRunDB,
        pfr: &PhysicalFunctionRunDB,
    ) -> Result<(), TdError> {
        let ltdvs: Vec<LogicalTableDataVersionDB> = db.live().fetch_all(&lfr.execution_id).await?;
        let tables: Vec<TableDB> = db.current().fetch_all::<TableDB>(&func.id).await?;
        for table in &tables {
            let ltdv = ltdvs
                .iter()
                .find(|l| l.table_id == table.table_id)
                .expect("one LTDV per output table");
            let (rows, bytes) = if *table.private { (7, 77) } else { (10, 100) };
            bind_ptdv_with_magnitudes(db, ltdv, table, pfr, rows, bytes).await?;
        }
        Ok(())
    }

    // ----- /inflight ---------------------------------------------------

    #[td_test::test(db)]
    async fn test_inflight_empty_table(db: TdPool) -> Result<(), TdError> {
        let req = InflightRequest {
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let rows = db.metrics().inflight(&req).await?;
        assert!(rows.is_empty(), "no runs yet, got {rows:?}");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_inflight_excludes_soft_deleted_collection(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let coll = seed_collection(&db, p.id, "inflight_del").await?;
        let func = make_publisher_function(&db, &coll, "inflight_del_fn").await?;
        let exec = db.scoped(p.id).seed().execution(&func).await?;
        let trx = db
            .scoped(p.id)
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        db.scoped(p.id)
            .seed()
            .physical_function_run(
                &coll,
                &func,
                &exec,
                &trx,
                &LogicalFunctionRunStatus::Running,
            )
            .await?;

        let req = InflightRequest {
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        assert!(
            !db.metrics().inflight(&req).await?.is_empty(),
            "a live collection's in-flight run must show"
        );

        soft_delete_collection(&db, &coll).await?;
        assert!(
            db.metrics().inflight(&req).await?.is_empty(),
            "a soft-deleted collection's in-flight run must be excluded"
        );
        Ok(())
    }

    // ----- /metrics nodes ----------------------------------------------

    #[td_test::test(db)]
    async fn test_nodes_empty_window(db: TdPool) -> Result<(), TdError> {
        let req = empty_request(Dim::Collection);
        let nodes = db.metrics().metrics_nodes(&req).await?;
        assert!(nodes.is_empty(), "empty DB → empty nodes, got {nodes:?}");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_nodes_publisher_out_magnitudes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (coll, _lfr, _ltdv) = seed_publisher_with_ltdv(&db, p.id, "pub_coll", "pub_fn").await?;

        let req = empty_request(Dim::Collection);
        let nodes = db.metrics().metrics_nodes(&req).await?;

        assert_eq!(
            nodes.len(),
            1,
            "one publisher → one node row, got {nodes:?}"
        );
        let n = &nodes[0];
        assert_eq!(n.collection_id.as_ref(), Some(&coll.id));
        assert_eq!(n.fn_runs, 1);
        // OUT magnitudes from PFR (stamp_buckets sets 1 / 10 / 100).
        assert_eq!(*n.table_versions_out.as_ref(), 1);
        assert_eq!(*n.row_count_out.as_ref(), 10);
        assert_eq!(*n.data_bytes_out.as_ref(), 100);
        // IN side empty for a pure publisher.
        assert_eq!(*n.table_versions_in.as_ref(), 0);
        assert_eq!(*n.row_count_in.as_ref(), 0);
        assert_eq!(*n.data_bytes_in.as_ref(), 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_nodes_out_magnitudes_cleared_by_void(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_coll, _lfr, ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "void_coll", "void_fn").await?;

        let req = empty_request(Dim::Collection);
        let before = db.metrics().metrics_nodes(&req).await?;
        assert_eq!(*before[0].row_count_out.as_ref(), 10);
        assert_eq!(*before[0].data_bytes_out.as_ref(), 100);

        // A later Voided event unbinds the LTDV in the pointer-current
        // snapshot, so the view stops resolving its PTDV magnitudes.
        let void = LogicalTableDataVersionPointerDB::build_event(
            ltdv.project_id,
            &ltdv.id,
            LogicalPointerStatus::Voided,
            None,
            AtTime::try_from(Timestamp::try_from(4_102_444_800_000_i64)?)?,
            ltdv.triggered_by_id,
            LogicalPointerReason::OperatorForceVoid,
        )?;
        db.scoped(ltdv.project_id)
            .live()
            .insert::<LogicalTableDataVersionPointerDB>(&void)
            .await?;

        let after = db.metrics().metrics_nodes(&req).await?;
        assert_eq!(
            after.len(),
            1,
            "the run still exists, only its binding is gone"
        );
        assert_eq!(
            *after[0].row_count_out.as_ref(),
            0,
            "void clears the bound PTDV row_count"
        );
        assert_eq!(
            *after[0].data_bytes_out.as_ref(),
            0,
            "void clears the bound PTDV data_bytes"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_node_output_tables_lists_distinct_output_table(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (coll, _lfr, ltdv) = seed_publisher_with_ltdv(&db, p.id, "lot_coll", "lot_fn").await?;

        let req = empty_request(Dim::Collection);
        let rows = db.metrics().metrics_node_output_tables(&req).await?;

        assert_eq!(
            rows.len(),
            1,
            "one publisher, one output table, got {rows:?}"
        );
        assert_eq!(rows[0].collection_id.as_ref(), Some(&coll.id));
        assert_eq!(rows[0].table_id, ltdv.table_id);
        assert_eq!(rows[0].status, LogicalFunctionRunStatus::Done);
        Ok(())
    }

    // ----- /metrics edges ----------------------------------------------

    #[td_test::test(db)]
    async fn test_edges_empty_window(db: TdPool) -> Result<(), TdError> {
        let req = empty_request(Dim::Collection);
        let edges = db.metrics().metrics_edges(&req).await?;
        assert!(edges.is_empty(), "empty DB → empty edges, got {edges:?}");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_edges_synthetic_for_publisher(db: TdPool) -> Result<(), TdError> {
        // A publisher whose output LTDV is never consumed produces TWO
        // edges in the closed-flow model: an ingress
        // (ExternalSource → coll) carrying the LTDV's magnitudes, and
        // an unused (coll → Unused) carrying the same magnitudes — the
        // bytes arrived and then hit a dead end.
        let p = db.seed().test_project().await?;
        let (coll, _, _ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "pub_synth", "pub_synth_fn").await?;

        let req = empty_request(Dim::Collection);
        let edges = db.metrics().metrics_edges(&req).await?;

        let ingress: Vec<_> = edges
            .iter()
            .filter(|e| e.source_kind == NodeKind::ExternalSource)
            .collect();
        let unused: Vec<_> = edges
            .iter()
            .filter(|e| e.target_kind == NodeKind::Unused)
            .collect();
        assert_eq!(ingress.len(), 1, "expected one ingress edge, got {edges:?}");
        assert_eq!(unused.len(), 1, "expected one unused edge, got {edges:?}");
        assert_eq!(edges.len(), 2, "no other edges expected, got {edges:?}");

        let ie = ingress[0];
        assert!(ie.source.is_none());
        assert_eq!(ie.target_kind, NodeKind::Dim);
        assert_eq!(ie.target, Some(coll.id.to_string()));
        assert_eq!(ie.fn_runs, 1);
        assert_eq!(ie.tables, 1);
        assert_eq!(*ie.row_count.as_ref(), 10);

        let ue = unused[0];
        assert_eq!(ue.source_kind, NodeKind::Dim);
        assert_eq!(ue.source, Some(coll.id.to_string()));
        assert!(ue.target.is_none());
        assert_eq!(ue.fn_runs, 1);
        assert_eq!(ue.tables, 1);
        assert_eq!(*ue.row_count.as_ref(), 10);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_edges_real_edge_source_ltdv_attribution(db: TdPool) -> Result<(), TdError> {
        // Producer LFR (publisher) creates an output LTDV; consumer LFR
        // references that LTDV via an LFRI row. The real edge's
        // magnitudes come from the SOURCE LTDV (producer's PTDV.row_count),
        // not from the consumer's own output.

        let p = db.seed().test_project().await?;
        let (producer_coll, _, producer_ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "real_prod", "real_prod_fn").await?;

        let consumer_coll = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("real_cons")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let consumer_fn = make_publisher_function(&db, &consumer_coll, "real_cons_fn").await?;
        let consumer_exec = db.scoped(p.id).seed().execution(&consumer_fn).await?;
        let consumer_trx = db
            .scoped(p.id)
            .seed()
            .transaction(&consumer_exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let consumer_pfr = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &consumer_coll,
                &consumer_fn,
                &consumer_exec,
                &consumer_trx,
                &LogicalFunctionRunStatus::Done,
            )
            .await?;
        stamp_buckets_on_pfr(&db, &consumer_pfr).await?;
        let consumer_lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one(and!(&consumer_pfr.execution_id, &consumer_pfr.function_id))
            .await?;

        let input = LogicalFunctionRunInputDB::builder()
            .collection_id(consumer_coll.id)
            .execution_id(&consumer_exec.id)
            .transaction_id(&consumer_trx.id)
            .logical_function_run_id(consumer_lfr.id.clone())
            .requirement_logical_table_data_version_id(Some(producer_ltdv.id))
            .requirement_input_idx(Some(InputIdx::try_from(0)?))
            .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
            .requirement_version_pos(VersionPos::try_from(0)?)
            .build()?;
        db.scoped(p.id).live().insert(&input).await?;

        let req = empty_request(Dim::Collection);
        let edges = db.metrics().metrics_edges(&req).await?;

        let real: Vec<_> = edges
            .iter()
            .filter(|e| e.source_kind == NodeKind::Dim && e.target_kind == NodeKind::Dim)
            .collect();
        assert_eq!(real.len(), 1, "one real edge expected, got {edges:?}");
        let e = real[0];
        assert_eq!(
            e.source.as_deref(),
            Some(producer_coll.id.to_string().as_str())
        );
        assert_eq!(e.target, Some(consumer_coll.id.to_string()));
        assert_eq!(e.fn_runs, 1);
        assert_eq!(e.tables, 1);
        // Source-LTDV attribution: 10 rows came from the producer's
        // output LTDV (not consumer's own 10 — same number here, but
        // attribution is source-side).
        assert_eq!(*e.row_count.as_ref(), 10);

        // The distinct-table listing backing `logical_tables` carries the same
        // real edge, one row for the producer's logical table.
        let edge_tables = db.metrics().metrics_edge_tables(&req).await?;
        let real_tbls: Vec<_> = edge_tables
            .iter()
            .filter(|t| t.source_kind == NodeKind::Dim && t.target_kind == NodeKind::Dim)
            .collect();
        assert_eq!(
            real_tbls.len(),
            1,
            "one (real edge, table) pair expected, got {edge_tables:?}"
        );
        assert_eq!(
            real_tbls[0].source.as_deref(),
            Some(producer_coll.id.to_string().as_str())
        );
        assert_eq!(real_tbls[0].target, Some(consumer_coll.id.to_string()));
        assert_eq!(real_tbls[0].table_id, producer_ltdv.table_id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_edges_egress_for_subscriber(db: TdPool) -> Result<(), TdError> {
        // Topological subscriber: consumes a producer's LTDV via LFRI
        // and produces NO output LTDVs of its own. Should emit an
        // egress edge (consumer_coll → ExternalDestination) with
        // source-LTDV-attributed magnitudes.
        let p = db.seed().test_project().await?;
        let (producer_coll, _, producer_ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "egress_prod", "egress_prod_fn").await?;

        let consumer_coll = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("egress_cons")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        // Subscriber function: same shape as publisher but no output
        // tables → no LTDVs minted at plan-build.
        let consumer_fn = {
            let create = td_objects::dxo::function::FunctionRegister::builder()
                .try_name("egress_cons_fn")?
                .try_description("test fn")?
                .bundle_id(BundleId::default())
                .try_snippet("snippet")?
                .decorator(Decorator::Subscriber)
                .dependencies(None)
                .triggers(None)
                .tables(Vec::<TableNameDto>::new())
                .try_runtime_values("rv")?
                .reuse_frozen_tables(false)
                .build()?;
            db.scoped(p.id)
                .seed()
                .function(&consumer_coll, &create)
                .await?
        };
        let consumer_exec = db.scoped(p.id).seed().execution(&consumer_fn).await?;
        let consumer_trx = db
            .scoped(p.id)
            .seed()
            .transaction(&consumer_exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let consumer_pfr = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &consumer_coll,
                &consumer_fn,
                &consumer_exec,
                &consumer_trx,
                &LogicalFunctionRunStatus::Done,
            )
            .await?;
        stamp_buckets_on_pfr(&db, &consumer_pfr).await?;
        let consumer_lfr: LogicalFunctionRunDB = db
            .live()
            .fetch_one(and!(&consumer_pfr.execution_id, &consumer_pfr.function_id))
            .await?;

        let input = LogicalFunctionRunInputDB::builder()
            .collection_id(consumer_coll.id)
            .execution_id(&consumer_exec.id)
            .transaction_id(&consumer_trx.id)
            .logical_function_run_id(consumer_lfr.id.clone())
            .requirement_logical_table_data_version_id(Some(producer_ltdv.id))
            .requirement_input_idx(Some(InputIdx::try_from(0)?))
            .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
            .requirement_version_pos(VersionPos::try_from(0)?)
            .build()?;
        db.scoped(p.id).live().insert(&input).await?;

        let req = empty_request(Dim::Collection);
        let edges = db.metrics().metrics_edges(&req).await?;

        let egress: Vec<_> = edges
            .iter()
            .filter(|e| e.target_kind == NodeKind::ExternalDestination)
            .collect();
        assert_eq!(egress.len(), 1, "expected one egress edge, got {edges:?}");
        let e = egress[0];
        assert_eq!(e.source_kind, NodeKind::Dim);
        assert_eq!(e.source, Some(consumer_coll.id.to_string()));
        assert!(e.target.is_none());
        assert_eq!(e.fn_runs, 1);
        assert_eq!(e.tables, 1);
        assert_eq!(*e.row_count.as_ref(), 10);
        // The producer's LTDV is consumed by the subscriber → NOT
        // unused. Confirms the anti-join on LFRI works.
        let unused: Vec<_> = edges
            .iter()
            .filter(|e| e.target_kind == NodeKind::Unused)
            .collect();
        assert!(
            unused.is_empty(),
            "consumed LTDV must not appear as unused, got {unused:?}"
        );
        // Producer is still ingress (no upstream).
        let ingress: Vec<_> = edges
            .iter()
            .filter(|e| e.source_kind == NodeKind::ExternalSource)
            .collect();
        assert_eq!(ingress.len(), 1);
        assert_eq!(ingress[0].target, Some(producer_coll.id.to_string()));
        Ok(())
    }

    // ----- private-table exclusion --------------------------------------

    #[td_test::test(db)]
    async fn test_private_output_excluded_from_nodes_and_edges(db: TdPool) -> Result<(), TdError> {
        // Publisher with a regular and a private output. The PFR metric
        // columns store the full worker totals (private included); the
        // /metrics response must only reflect the regular table.
        let p = db.seed().test_project().await?;
        let coll = seed_collection(&db, p.id, "priv_out").await?;
        let func =
            make_publisher_function_with_tables(&db, &coll, "priv_out_fn", &["out", "_priv"])
                .await?;
        let (lfr, pfr) = seed_done_run(&db, &coll, &func).await?;
        stamp_buckets_and_metrics_on_pfr(&db, &pfr, 2, 17, 177).await?;
        bind_all_outputs(&db, &func, &lfr, &pfr).await?;

        let nodes = db
            .metrics()
            .metrics_nodes(&empty_request(Dim::Collection))
            .await?;
        assert_eq!(nodes.len(), 1, "one node expected, got {nodes:?}");
        let n = &nodes[0];
        assert_eq!(
            *n.table_versions_out.as_ref(),
            1,
            "private output must not count, got {n:?}"
        );
        assert_eq!(*n.row_count_out.as_ref(), 10);
        assert_eq!(*n.data_bytes_out.as_ref(), 100);

        let edges = db
            .metrics()
            .metrics_edges(&empty_request(Dim::Collection))
            .await?;
        let ingress: Vec<_> = edges
            .iter()
            .filter(|e| e.source_kind == NodeKind::ExternalSource)
            .collect();
        let unused: Vec<_> = edges
            .iter()
            .filter(|e| e.target_kind == NodeKind::Unused)
            .collect();
        assert_eq!(ingress.len(), 1, "one ingress edge, got {edges:?}");
        assert_eq!(ingress[0].tables, 1);
        assert_eq!(*ingress[0].row_count.as_ref(), 10);
        assert_eq!(unused.len(), 1, "one unused edge, got {edges:?}");
        assert_eq!(unused[0].tables, 1);
        assert_eq!(*unused[0].row_count.as_ref(), 10);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_private_input_excluded_from_in_and_real_edges(db: TdPool) -> Result<(), TdError> {
        // Producer emits ONLY a private table; a consumer in another
        // collection references it via LFRI. The private LTDV must not
        // surface anywhere: no IN magnitudes, no real edge, no ingress
        // for the producer, no unused edge for the private output.
        let p = db.seed().test_project().await?;
        let prod_coll = seed_collection(&db, p.id, "priv_in_prod").await?;
        let prod_fn =
            make_publisher_function_with_tables(&db, &prod_coll, "priv_in_prod_fn", &["_priv"])
                .await?;
        let (prod_lfr, prod_pfr) = seed_done_run(&db, &prod_coll, &prod_fn).await?;
        stamp_buckets_on_pfr(&db, &prod_pfr).await?;
        bind_all_outputs(&db, &prod_fn, &prod_lfr, &prod_pfr).await?;
        let prod_ltdvs: Vec<LogicalTableDataVersionDB> =
            db.live().fetch_all(&prod_lfr.execution_id).await?;
        let private_ltdv = &prod_ltdvs[0];

        let cons_coll = seed_collection(&db, p.id, "priv_in_cons").await?;
        let cons_fn = make_publisher_function(&db, &cons_coll, "priv_in_cons_fn").await?;
        let (cons_lfr, cons_pfr) = seed_done_run(&db, &cons_coll, &cons_fn).await?;
        stamp_buckets_on_pfr(&db, &cons_pfr).await?;

        let input = LogicalFunctionRunInputDB::builder()
            .collection_id(cons_coll.id)
            .execution_id(&cons_lfr.execution_id)
            .transaction_id(&cons_lfr.transaction_id)
            .logical_function_run_id(cons_lfr.id.clone())
            .requirement_logical_table_data_version_id(Some(private_ltdv.id.clone()))
            .requirement_input_idx(Some(InputIdx::try_from(0)?))
            .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
            .requirement_version_pos(VersionPos::try_from(0)?)
            .build()?;
        db.scoped(p.id).live().insert(&input).await?;

        let nodes = db
            .metrics()
            .metrics_nodes(&empty_request(Dim::Collection))
            .await?;
        let prod_node = nodes
            .iter()
            .find(|n| n.collection_id.as_ref() == Some(&prod_coll.id))
            .expect("producer node");
        assert_eq!(
            *prod_node.table_versions_out.as_ref(),
            0,
            "private-only producer must report zero OUT, got {prod_node:?}"
        );
        assert_eq!(*prod_node.row_count_out.as_ref(), 0);
        let cons_node = nodes
            .iter()
            .find(|n| n.collection_id.as_ref() == Some(&cons_coll.id))
            .expect("consumer node");
        assert_eq!(
            *cons_node.table_versions_in.as_ref(),
            0,
            "private input must not count, got {cons_node:?}"
        );
        assert_eq!(*cons_node.row_count_in.as_ref(), 0);

        let edges = db
            .metrics()
            .metrics_edges(&empty_request(Dim::Collection))
            .await?;
        let prod_id = prod_coll.id.to_string();
        assert!(
            edges
                .iter()
                .all(|e| e.source.as_deref() != Some(prod_id.as_str())
                    && e.target.as_deref() != Some(prod_id.as_str())),
            "private-only producer must have no edges, got {edges:?}"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_private_only_outputs_make_topological_subscriber(
        db: TdPool,
    ) -> Result<(), TdError> {
        // A consumer whose only outputs are private has no visible
        // outputs: /metrics classifies it as a topological subscriber
        // and reports its consumed input as an egress edge.
        let p = db.seed().test_project().await?;
        let (_prod_coll, _, prod_ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "priv_egr_prod", "priv_egr_prod_fn").await?;

        let cons_coll = seed_collection(&db, p.id, "priv_egr_cons").await?;
        let cons_fn =
            make_publisher_function_with_tables(&db, &cons_coll, "priv_egr_cons_fn", &["_state"])
                .await?;
        let (cons_lfr, cons_pfr) = seed_done_run(&db, &cons_coll, &cons_fn).await?;
        stamp_buckets_on_pfr(&db, &cons_pfr).await?;

        let input = LogicalFunctionRunInputDB::builder()
            .collection_id(cons_coll.id)
            .execution_id(&cons_lfr.execution_id)
            .transaction_id(&cons_lfr.transaction_id)
            .logical_function_run_id(cons_lfr.id.clone())
            .requirement_logical_table_data_version_id(Some(prod_ltdv.id))
            .requirement_input_idx(Some(InputIdx::try_from(0)?))
            .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
            .requirement_version_pos(VersionPos::try_from(0)?)
            .build()?;
        db.scoped(p.id).live().insert(&input).await?;

        let edges = db
            .metrics()
            .metrics_edges(&empty_request(Dim::Collection))
            .await?;
        let egress: Vec<_> = edges
            .iter()
            .filter(|e| e.target_kind == NodeKind::ExternalDestination)
            .collect();
        assert_eq!(egress.len(), 1, "one egress edge, got {edges:?}");
        assert_eq!(egress[0].source, Some(cons_coll.id.to_string()));
        assert_eq!(*egress[0].row_count.as_ref(), 10);
        // The consumer's private output must not show as unused.
        assert!(
            edges
                .iter()
                .all(|e| e.target_kind != NodeKind::Unused
                    || e.source != Some(cons_coll.id.to_string())),
            "private output must not appear as unused, got {edges:?}"
        );
        Ok(())
    }

    // ----- soft-deleted collection/project exclusion --------------------

    #[td_test::test(db)]
    async fn test_soft_deleted_collection_excluded_from_nodes_and_edges(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (coll, _lfr, _ltdv) = seed_publisher_with_ltdv(&db, p.id, "del_coll", "del_fn").await?;
        let req = empty_request(Dim::Collection);
        assert_eq!(db.metrics().metrics_nodes(&req).await?.len(), 1);
        assert!(!db.metrics().metrics_edges(&req).await?.is_empty());

        soft_delete_collection(&db, &coll).await?;
        assert!(
            db.metrics().metrics_nodes(&req).await?.is_empty(),
            "a soft-deleted collection must not appear as a node"
        );
        assert!(
            db.metrics().metrics_edges(&req).await?.is_empty(),
            "a soft-deleted collection's edges must vanish"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_soft_deleted_project_excluded_from_nodes(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (_coll, _lfr, _ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "del_proj_coll", "del_proj_fn").await?;
        let req = empty_request(Dim::Collection);
        assert_eq!(db.metrics().metrics_nodes(&req).await?.len(), 1);

        soft_delete_project(&db, &p).await?;
        assert!(
            db.metrics().metrics_nodes(&req).await?.is_empty(),
            "runs under a soft-deleted project must be excluded even when the collection is live"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_soft_deleted_source_collection_excluded_from_real_edge(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (producer_coll, _, producer_ltdv) =
            seed_publisher_with_ltdv(&db, p.id, "edge_del_prod", "edge_del_prod_fn").await?;
        let consumer_coll = seed_collection(&db, p.id, "edge_del_cons").await?;
        let consumer_fn = make_publisher_function(&db, &consumer_coll, "edge_del_cons_fn").await?;
        let (consumer_lfr, consumer_pfr) = seed_done_run(&db, &consumer_coll, &consumer_fn).await?;
        stamp_buckets_on_pfr(&db, &consumer_pfr).await?;
        let input = LogicalFunctionRunInputDB::builder()
            .collection_id(consumer_coll.id)
            .execution_id(&consumer_lfr.execution_id)
            .transaction_id(&consumer_lfr.transaction_id)
            .logical_function_run_id(consumer_lfr.id.clone())
            .requirement_logical_table_data_version_id(Some(producer_ltdv.id))
            .requirement_input_idx(Some(InputIdx::try_from(0)?))
            .requirement_dependency_pos(Some(DependencyPos::try_from(0)?))
            .requirement_version_pos(VersionPos::try_from(0)?)
            .build()?;
        db.scoped(p.id).live().insert(&input).await?;

        let req = empty_request(Dim::Collection);
        let producer_id = producer_coll.id.to_string();
        assert!(
            db.metrics()
                .metrics_edges(&req)
                .await?
                .iter()
                .any(|e| e.source.as_deref() == Some(producer_id.as_str())),
            "the live producer must be a real-edge source before deletion"
        );

        soft_delete_collection(&db, &producer_coll).await?;
        let edges = db.metrics().metrics_edges(&req).await?;
        assert!(
            edges
                .iter()
                .all(|e| e.source.as_deref() != Some(producer_id.as_str())
                    && e.target.as_deref() != Some(producer_id.as_str())),
            "no edge may reference the soft-deleted producer, got {edges:?}"
        );
        let cons_node = db
            .metrics()
            .metrics_nodes(&req)
            .await?
            .into_iter()
            .any(|n| n.collection_id.as_ref() == Some(&consumer_coll.id));
        assert!(cons_node, "the live consumer must still be a node");
        Ok(())
    }
}
