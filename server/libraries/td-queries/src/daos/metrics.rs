//
// Copyright 2026 Tabs Data Inc.
//

//! Operational-metrics queries.
//!
//! Backs the consolidated `/metrics` endpoint (nodes + edges per
//! `(bucket, dim, status)`) and `/inflight` (point-in-time, no window).
//! All queries are pure SELECT statements built via sea_query — no
//! `format!`, no dialect-specific syntax.
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
//! ## System-table exclusion invariant
//!
//! Anywhere LTDVs/PTDVs are touched we filter
//! `LTDV.function_param_pos >= 0` (mirrors the `system` derivation in
//! the schema view). Input-side joins additionally enforce
//! `LFRI.dep_pos >= 0`, which excludes `td.fn_state__*` plumbing on
//! the consumer side. Output-side magnitudes use the worker-side
//! aggregated PFR columns, which exclude `system_output` by
//! construction.

use crate::QueryExecutor;
use chrono::{DateTime, Utc};
use sea_query::{
    Alias, Expr, ExprTrait, Func, IntoIden, JoinType, NullOrdering, Order, Query, SelectStatement,
    SimpleExpr, UnionType, Value,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::collection::CollectionDBIden;
use td_objects::dxo::group::GroupDBIden;
use td_objects::dxo::logical::{
    FunctionRunDBWithNames, FunctionRunDBWithNamesIden, LogicalFunctionRunDB,
    LogicalFunctionRunDBIden, LogicalFunctionRunInputDBIden, LogicalTableDataVersionDBIden,
    TableDataVersionDBWithNamesIden,
};
use td_objects::dxo::metrics::{
    Dim, EdgeRow, InflightRequest, InflightRow, MetricsRequest, NodeKind, NodeRow,
};
use td_objects::dxo::query_filters::{DimensionFilter, WindowUnit};
use td_objects::types::basic::LogicalFunctionRunStatus;

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
    /// from PFR (worker-aggregated, system-excluded). `*_in` magnitudes
    /// come from the LFRI subquery joined to source-LTDV via the LTDV
    /// view; system tables excluded via `dep_pos >= 0` and
    /// `function_param_pos >= 0`.
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
}

// ---------------------------------------------------------------------------
// /metrics — nodes
// ---------------------------------------------------------------------------

/// Per-`(bucket, dim_id, group_id, status)` node aggregator. IN
/// magnitudes come from a LEFT JOIN'd subquery that aggregates LFRI
/// rows per LFR; the outer aggregator sums them into per-node totals.
fn node_aggregates_query(req: &MetricsRequest) -> SelectStatement {
    let bucket_iden = bucket_column(req.unit);
    let in_alias = Alias::new("ins").into_iden();

    let mut stmt = Query::select();

    // Bucket axis
    stmt.expr_as(
        Expr::col((FunctionRunDBWithNamesIden::Table, bucket_iden)),
        Alias::new("bucket"),
    );

    // Real-dim branch — tag every row with NodeKind::Dim. The three
    // synthetic NodeRow kinds (ExternalSource / ExternalDestination /
    // Unused) are NOT produced by SQL; they are derived in the
    // service layer from the edge stream — see
    // `td_services::metrics::layers::synthesize_nodes_from_edges`.
    stmt.expr_as(kind_lit(NodeKind::Dim), Alias::new("kind"));

    // Dim-conditional projection: collection_id is only present when
    // dim=collection. dim=group rows return NULL.
    let collection_id_expr: SimpleExpr = match req.dim {
        Dim::Collection => Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        )),
        Dim::Group => Expr::value(Value::String(None)),
    };
    stmt.expr_as(collection_id_expr, Alias::new("collection_id"));

    // NULL on group-dim rows to match collection_id's projection.
    let collection_name_expr: SimpleExpr = match req.dim {
        Dim::Collection => Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Collection,
        )),
        Dim::Group => Expr::value(Value::String(None)),
    };
    stmt.expr_as(collection_name_expr, Alias::new("collection_name"));

    // group_id is always projected. Doubles as dim_id when dim=group;
    // otherwise it's metadata letting clients bucket nodes into
    // source/destination/etc. without a separate /groups call.
    stmt.expr_as(
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::GroupId,
        )),
        Alias::new("group_id"),
    );

    stmt.expr_as(
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Group,
        )),
        Alias::new("group_name"),
    );

    stmt.expr_as(
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        )),
        Alias::new("status"),
    );

    stmt.expr_as(Func::count(Expr::val(1)), Alias::new("fn_runs"));
    stmt.expr_as(
        Func::count_distinct(Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::TransactionId,
        ))),
        Alias::new("transactions"),
    );

    // OUT magnitudes — worker-aggregated, system-excluded.
    stmt.expr_as(
        coalesce_sum((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::MetricTableVersions,
        )),
        Alias::new("table_versions_out"),
    );
    stmt.expr_as(
        coalesce_sum((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::MetricRows,
        )),
        Alias::new("row_count_out"),
    );
    stmt.expr_as(
        coalesce_sum((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::MetricDataBytes,
        )),
        Alias::new("data_bytes_out"),
    );

    // IN magnitudes from the per-LFR aggregates subquery.
    stmt.expr_as(
        coalesce_sum((in_alias.clone(), Alias::new("tables_in"))),
        Alias::new("table_versions_in"),
    );
    stmt.expr_as(
        coalesce_sum((in_alias.clone(), Alias::new("rows_in"))),
        Alias::new("row_count_in"),
    );
    stmt.expr_as(
        coalesce_sum((in_alias.clone(), Alias::new("bytes_in"))),
        Alias::new("data_bytes_in"),
    );

    stmt.from(FunctionRunDBWithNamesIden::Table).join_subquery(
        JoinType::LeftJoin,
        per_lfr_input_aggregates_subquery(req),
        in_alias.clone(),
        Expr::col((in_alias.clone(), Alias::new("lfr_id"))).equals((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Id,
        )),
    );

    apply_ended_on_window(&mut stmt, req.window.start_ms, req.window.end_ms);
    apply_status_filter(
        &mut stmt,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ),
    );
    // Target-only filters on nodes (a node IS its dim entity; there's
    // no second side to filter).
    apply_dim_filter(
        &mut stmt,
        &req.collections,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ),
    );
    apply_dim_filter(
        &mut stmt,
        &req.groups,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::GroupId,
        ),
    );

    // GROUP BY: bucket + status always; collection_id only when
    // dim=collection (to preserve per-collection rows); group_id
    // always (per dim it's either the dim id or metadata that varies
    // with collection-group reassignment events within the window).
    stmt.group_by_col((FunctionRunDBWithNamesIden::Table, bucket_iden));
    if matches!(req.dim, Dim::Collection) {
        stmt.group_by_col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ));
        // Functionally dependent on collection_id, but SQL still
        // requires it in GROUP BY since it's projected.
        stmt.group_by_col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Collection,
        ));
    }
    stmt.group_by_col((
        FunctionRunDBWithNamesIden::Table,
        FunctionRunDBWithNamesIden::GroupId,
    ));
    stmt.group_by_col((
        FunctionRunDBWithNamesIden::Table,
        FunctionRunDBWithNamesIden::Group,
    ));
    stmt.group_by_col((
        FunctionRunDBWithNamesIden::Table,
        FunctionRunDBWithNamesIden::Status,
    ));

    stmt.order_by(
        (FunctionRunDBWithNamesIden::Table, bucket_iden),
        Order::Desc,
    );
    stmt
}

/// Subquery: per-LFR aggregates over qualifying LFRI inputs. Each
/// output row carries `(lfr_id, tables_in, rows_in, bytes_in)`. Joined
/// LEFT-side from the outer node query — LFRs with no inputs (pure
/// publishers) get zeros via the outer COALESCE.
///
/// Scoped to the request's window via an INNER JOIN to the LFR view,
/// so the GROUP BY only aggregates LFRIs whose LFR ended inside the
/// window — without this scoping the subquery would aggregate the
/// entire historic LFRI table before the LEFT JOIN narrowed it.
fn per_lfr_input_aggregates_subquery(req: &MetricsRequest) -> SelectStatement {
    let mut stmt = Query::select();
    stmt.expr_as(
        Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        )),
        Alias::new("lfr_id"),
    )
    .expr_as(
        Func::count_distinct(Expr::col((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        ))),
        Alias::new("tables_in"),
    )
    .expr_as(
        coalesce_sum((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::RowCount,
        )),
        Alias::new("rows_in"),
    )
    .expr_as(
        coalesce_sum((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::DataBytes,
        )),
        Alias::new("bytes_in"),
    )
    .from(LogicalFunctionRunInputDBIden::Table)
    // Scope to window: only LFRIs whose consumer LFR ended in window.
    .join(
        JoinType::InnerJoin,
        FunctionRunDBWithNamesIden::Table,
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Id,
        ))
        .equals((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
        )),
    )
    .join(
        JoinType::InnerJoin,
        TableDataVersionDBWithNamesIden::Table,
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::Id,
        ))
        .equals((
            LogicalFunctionRunInputDBIden::Table,
            LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
        )),
    )
    // User dep edges only — excludes system inputs (dep_pos = -1) and
    // trigger-only edges (dep_pos NULL).
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
    )
    // Defensive system-table exclusion alongside `dep_pos >= 0`.
    .and_where(
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::FunctionParamPos,
        ))
        .gte(0),
    )
    .group_by_col((
        LogicalFunctionRunInputDBIden::Table,
        LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
    ));

    apply_ended_on_window(&mut stmt, req.window.start_ms, req.window.end_ms);
    stmt
}

// ---------------------------------------------------------------------------
// /metrics — edges (UNION ALL of real + synthetic branches)
// ---------------------------------------------------------------------------

fn edge_aggregates_query(req: &MetricsRequest) -> SelectStatement {
    let inner_alias = Alias::new("e").into_iden();

    let mut inner = edge_real_branch(req);
    inner.union(UnionType::All, edge_synth_branch(req));

    let mut outer = Query::select();
    outer
        .expr(Expr::col((inner_alias.clone(), Alias::new("bucket"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("source_kind"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("source"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("source_name"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("target_kind"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("target"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("target_name"))))
        .expr(Expr::col((inner_alias.clone(), Alias::new("status"))))
        .expr_as(
            Func::count_distinct(Expr::col((inner_alias.clone(), Alias::new("lfr_id")))),
            Alias::new("fn_runs"),
        )
        .expr_as(
            Func::count_distinct(Expr::col((
                inner_alias.clone(),
                Alias::new("source_ltdv_id"),
            ))),
            Alias::new("tables"),
        )
        .expr_as(
            coalesce_sum((inner_alias.clone(), Alias::new("source_row_count"))),
            Alias::new("row_count"),
        )
        .expr_as(
            coalesce_sum((inner_alias.clone(), Alias::new("source_data_bytes"))),
            Alias::new("data_bytes"),
        )
        .from_subquery(inner, inner_alias.clone())
        .group_by_columns([
            (inner_alias.clone(), Alias::new("bucket").into_iden()),
            (inner_alias.clone(), Alias::new("source_kind").into_iden()),
            (inner_alias.clone(), Alias::new("source").into_iden()),
            (inner_alias.clone(), Alias::new("source_name").into_iden()),
            (inner_alias.clone(), Alias::new("target_kind").into_iden()),
            (inner_alias.clone(), Alias::new("target").into_iden()),
            (inner_alias.clone(), Alias::new("target_name").into_iden()),
            (inner_alias.clone(), Alias::new("status").into_iden()),
        ])
        // Synthetic-endpoint edges have NULL on one side; force
        // NULLS LAST so the ordering is dialect-independent
        // (Postgres defaults to NULLS LAST for ASC, SQLite to
        // NULLS FIRST). Stable cross-backend ordering matters for
        // snapshot tests and any client rendering in insertion order.
        .order_by((inner_alias.clone(), Alias::new("bucket")), Order::Desc)
        .order_by_with_nulls(
            (inner_alias.clone(), Alias::new("source")),
            Order::Asc,
            NullOrdering::Last,
        )
        .order_by_with_nulls(
            (inner_alias.clone(), Alias::new("target")),
            Order::Asc,
            NullOrdering::Last,
        )
        .order_by((inner_alias, Alias::new("status")), Order::Asc);

    outer
}

/// Real-edges branch: each row is a (consumer LFR, source LTDV) pair
/// projected with the source dim id, target dim id, status, plus the
/// LTDV's bound-PTDV magnitudes. The outer aggregator dedupes by
/// `lfr_id` for fn_runs and by `source_ltdv_id` for tables.
fn edge_real_branch(req: &MetricsRequest) -> SelectStatement {
    let bucket_iden = bucket_column(req.unit);
    let source_dim_iden = source_dim_iden(req.dim);
    let target_dim_iden = target_dim_iden(req.dim);
    let source_dim_name_iden = source_dim_name_iden(req.dim);
    let target_dim_name_iden = target_dim_name_iden(req.dim);

    let mut stmt = Query::select();
    stmt.distinct()
        .expr_as(
            Expr::col((FunctionRunDBWithNamesIden::Table, bucket_iden)),
            Alias::new("bucket"),
        )
        .expr_as(kind_lit(NodeKind::Dim), Alias::new("source_kind"))
        .expr_as(
            Expr::col((TableDataVersionDBWithNamesIden::Table, source_dim_iden)),
            Alias::new("source"),
        )
        .expr_as(
            Expr::col((TableDataVersionDBWithNamesIden::Table, source_dim_name_iden)),
            Alias::new("source_name"),
        )
        .expr_as(kind_lit(NodeKind::Dim), Alias::new("target_kind"))
        .expr_as(
            Expr::col((FunctionRunDBWithNamesIden::Table, target_dim_iden)),
            Alias::new("target"),
        )
        .expr_as(
            Expr::col((FunctionRunDBWithNamesIden::Table, target_dim_name_iden)),
            Alias::new("target_name"),
        )
        .expr_as(
            Expr::col((
                FunctionRunDBWithNamesIden::Table,
                FunctionRunDBWithNamesIden::Status,
            )),
            Alias::new("status"),
        )
        .expr_as(
            Expr::col((
                FunctionRunDBWithNamesIden::Table,
                FunctionRunDBWithNamesIden::Id,
            )),
            Alias::new("lfr_id"),
        )
        .expr_as(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                LogicalTableDataVersionDBIden::Id,
            )),
            Alias::new("source_ltdv_id"),
        )
        .expr_as(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::RowCount,
            )),
            Alias::new("source_row_count"),
        )
        .expr_as(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                TableDataVersionDBWithNamesIden::DataBytes,
            )),
            Alias::new("source_data_bytes"),
        )
        .from(FunctionRunDBWithNamesIden::Table)
        .join(
            JoinType::InnerJoin,
            LogicalFunctionRunInputDBIden::Table,
            Expr::col((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::LogicalFunctionRunId,
            ))
            .equals((
                FunctionRunDBWithNamesIden::Table,
                FunctionRunDBWithNamesIden::Id,
            )),
        )
        .join(
            JoinType::InnerJoin,
            TableDataVersionDBWithNamesIden::Table,
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                LogicalTableDataVersionDBIden::Id,
            ))
            .equals((
                LogicalFunctionRunInputDBIden::Table,
                LogicalFunctionRunInputDBIden::RequirementLogicalTableDataVersionId,
            )),
        )
        // Qualifications.
        .and_where(
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
        )
        // System-table exclusion (defense-in-depth alongside dep_pos).
        .and_where(
            Expr::col((
                TableDataVersionDBWithNamesIden::Table,
                LogicalTableDataVersionDBIden::FunctionParamPos,
            ))
            .gte(0),
        )
        // Self-loops dropped.
        .and_where(
            Expr::col((TableDataVersionDBWithNamesIden::Table, source_dim_iden)).ne(Expr::col((
                FunctionRunDBWithNamesIden::Table,
                target_dim_iden,
            ))),
        );

    apply_ended_on_window(&mut stmt, req.window.start_ms, req.window.end_ms);
    apply_status_filter(
        &mut stmt,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ),
    );
    // Either-endpoint filters on real edges (source OR target passes
    // include; BOTH must pass exclude).
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

/// Synthetic-external-edges branch: publisher LFRs (no qualifying
/// upstream user-dep input) projected against their OWN output LTDVs.
/// `source` is NULL; magnitudes describe data the publisher pushed
/// into its target dim from outside Tabsdata.
fn edge_synth_branch(req: &MetricsRequest) -> SelectStatement {
    let bucket_iden = bucket_column(req.unit);
    let target_dim_iden = target_dim_iden(req.dim);
    let target_dim_name_iden = target_dim_name_iden(req.dim);

    // Typed NULLs so the UNION ALL column types match the real branch.
    let null_source: SimpleExpr = Expr::value(Value::String(None));
    let null_source_name: SimpleExpr = Expr::value(Value::String(None));

    // No DISTINCT: anti-join on LFRI + unique LTDV.producer_lfr_id means
    // each (LFR, output-LTDV) pair appears at most once.
    let mut stmt = Query::select();
    stmt.expr_as(
        Expr::col((FunctionRunDBWithNamesIden::Table, bucket_iden)),
        Alias::new("bucket"),
    )
    .expr_as(kind_lit(NodeKind::ExternalSource), Alias::new("source_kind"))
    .expr_as(null_source, Alias::new("source"))
    .expr_as(null_source_name, Alias::new("source_name"))
    .expr_as(kind_lit(NodeKind::Dim), Alias::new("target_kind"))
    .expr_as(
        Expr::col((FunctionRunDBWithNamesIden::Table, target_dim_iden)),
        Alias::new("target"),
    )
    .expr_as(
        Expr::col((FunctionRunDBWithNamesIden::Table, target_dim_name_iden)),
        Alias::new("target_name"),
    )
    .expr_as(
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        )),
        Alias::new("status"),
    )
    .expr_as(
        Expr::col((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Id,
        )),
        Alias::new("lfr_id"),
    )
    .expr_as(
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::Id,
        )),
        Alias::new("source_ltdv_id"),
    )
    .expr_as(
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::RowCount,
        )),
        Alias::new("source_row_count"),
    )
    .expr_as(
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            TableDataVersionDBWithNamesIden::DataBytes,
        )),
        Alias::new("source_data_bytes"),
    )
    .from(FunctionRunDBWithNamesIden::Table)
    // Publisher's OWN output LTDVs (LTDV.producer_lfr_id = fr.id).
    .join(
        JoinType::InnerJoin,
        TableDataVersionDBWithNamesIden::Table,
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::ProducerLogicalFunctionRunId,
        ))
        .equals((
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Id,
        )),
    )
    // Anti-join: no qualifying upstream LFRI (= publisher condition).
    .join(
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
    )
    // System-table exclusion on the publisher's output LTDVs.
    .and_where(
        Expr::col((
            TableDataVersionDBWithNamesIden::Table,
            LogicalTableDataVersionDBIden::FunctionParamPos,
        ))
        .gte(0),
    );

    apply_ended_on_window(&mut stmt, req.window.start_ms, req.window.end_ms);
    apply_status_filter(
        &mut stmt,
        &req.statuses,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::Status,
        ),
    );
    // Synthetic edges have no source dim → target-only dim filters.
    apply_dim_filter(
        &mut stmt,
        &req.collections,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::CollectionId,
        ),
    );
    apply_dim_filter(
        &mut stmt,
        &req.groups,
        (
            FunctionRunDBWithNamesIden::Table,
            FunctionRunDBWithNamesIden::GroupId,
        ),
    );

    stmt
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
        Expr::col((CollectionDBIden::Table, CollectionDBIden::Name)),
        Alias::new("collection_name"),
    )
    .column((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::GroupId,
    ))
    .expr_as(
        Expr::col((GroupDBIden::Table, GroupDBIden::Name)),
        Alias::new("group_name"),
    )
    .column((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::Status,
    ))
    .expr_as(Func::count(Expr::val(1)), Alias::new("count"))
    .from(LogicalFunctionRunDBIden::Table)
    .join(
        JoinType::LeftJoin,
        CollectionDBIden::Table,
        Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::CollectionId,
        ))
        .equals((CollectionDBIden::Table, CollectionDBIden::Id)),
    )
    .join(
        JoinType::LeftJoin,
        GroupDBIden::Table,
        Expr::col((
            LogicalFunctionRunDBIden::Table,
            LogicalFunctionRunDBIden::GroupId,
        ))
        .equals((GroupDBIden::Table, GroupDBIden::Id)),
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
    .group_by_col((CollectionDBIden::Table, CollectionDBIden::Name))
    .group_by_col((
        LogicalFunctionRunDBIden::Table,
        LogicalFunctionRunDBIden::GroupId,
    ))
    .group_by_col((GroupDBIden::Table, GroupDBIden::Name))
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

/// The precomputed `ended_on_<unit>_ms` column to `GROUP BY` for this
/// window unit. `Quarter` / `Year` are not stored columns; they fall
/// back to `EndedOnMonthMs` and the service layer folds monthly rows
/// up into quarter / year buckets.
fn bucket_column(unit: WindowUnit) -> FunctionRunDBWithNamesIden {
    match unit {
        WindowUnit::Hour => FunctionRunDBWithNamesIden::EndedOnHourMs,
        WindowUnit::Day => FunctionRunDBWithNamesIden::EndedOnDayMs,
        WindowUnit::Week => FunctionRunDBWithNamesIden::EndedOnWeekMs,
        WindowUnit::Month | WindowUnit::Quarter | WindowUnit::Year => {
            FunctionRunDBWithNamesIden::EndedOnMonthMs
        }
    }
}

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

/// Per-dim source-side name column (LTDV-with-names view).
fn source_dim_name_iden(dim: Dim) -> TableDataVersionDBWithNamesIden {
    match dim {
        Dim::Collection => TableDataVersionDBWithNamesIden::Collection,
        Dim::Group => TableDataVersionDBWithNamesIden::Group,
    }
}

/// Per-dim target-side name column (LFR-with-names view).
fn target_dim_name_iden(dim: Dim) -> FunctionRunDBWithNamesIden {
    match dim {
        Dim::Collection => FunctionRunDBWithNamesIden::Collection,
        Dim::Group => FunctionRunDBWithNamesIden::Group,
    }
}

/// Convert UTC epoch ms to `DateTime<Utc>` for binding against the
/// `ended_on` TIMESTAMPTZ column. Anchor-aligned windows can have
/// sub-bucket bounds, so we filter on the raw timestamp and only use
/// the bucket-floored column for `GROUP BY`.
fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).expect("window bound must be a valid UTC timestamp")
}

/// `WHERE status IN ENDED AND ended_on >= start AND ended_on < end`.
///
/// The status predicate is *logically* redundant in steady state —
/// the schema contract is `status ∈ ENDED ↔ ended_on IS NOT NULL on
/// the bound PFR` — but stating it makes the "exclude in-flight"
/// intent self-documenting and closes the Voided/Pending pointer
/// corner case where the bound PFR (and thus `ended_on`) is gone
/// while the LFR's sticky status may still be in ENDED.
fn apply_ended_on_window(stmt: &mut SelectStatement, start_ms: i64, end_ms: i64) {
    let status = (
        FunctionRunDBWithNamesIden::Table,
        FunctionRunDBWithNamesIden::Status,
    );
    let ended_on = (
        FunctionRunDBWithNamesIden::Table,
        FunctionRunDBWithNamesIden::EndedOn,
    );
    let ended_vals: Vec<_> = LogicalFunctionRunStatus::ENDED
        .iter()
        .map(SeaValue::as_sea_value)
        .collect();
    stmt.and_where(Expr::col(status).is_in(ended_vals))
        .and_where(Expr::col(ended_on).gte(ms_to_dt(start_ms)))
        .and_where(Expr::col(ended_on).lt(ms_to_dt(end_ms)));
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
    use td_database::pool::TdPool;
    use td_error::TdError;
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
    use td_objects::dxo::query_filters::DimensionFilter;
    use td_objects::dxo::query_filters::{TimeWindow, WindowUnit};
    use td_objects::dxo::table::TableDB;
    use td_objects::types::basic::{
        AtTime, BundleId, ByteCount, Decorator, DependencyPos, EpochMs, HasData, InputIdx,
        LogicalFunctionRunStatus, LogicalPointerReason, LogicalPointerStatus, RowCount,
        TableFunctionParamPos, TableNameDto, TableVersionCount, TransactionKey, VersionPos,
    };
    use td_objects::types::basic::{CollectionName, UserId};
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
        collection: &td_objects::dxo::collection::CollectionDB,
        name: &str,
    ) -> Result<FunctionDB, TdError> {
        let create = FunctionRegister::builder()
            .try_name(name)?
            .try_description("test fn")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("out")?])
            .try_runtime_values("rv")?
            .reuse_frozen_tables(false)
            .build()?;
        db.seed().function(collection, &create).await
    }

    async fn stamp_buckets_on_pfr(db: &TdPool, pfr: &PhysicalFunctionRunDB) -> Result<(), TdError> {
        let bucket = EpochMs::try_from(IN_WINDOW)?;
        let ended_on = AtTime::try_from(td_common::datetime::Timestamp::try_from(IN_WINDOW)?)?;
        let update = UpdatePhysicalFunctionRunDB::builder()
            .status(td_objects::types::basic::PhysicalFunctionRunStatus::Done)
            .ended_on(Some(ended_on))
            .ended_on_hour_ms(Some(bucket))
            .ended_on_day_ms(Some(bucket))
            .ended_on_week_ms(Some(bucket))
            .ended_on_month_ms(Some(bucket))
            .metric_table_versions(Some(TableVersionCount::try_from(1)?))
            .metric_rows(Some(RowCount::try_from(10)?))
            .metric_data_bytes(Some(ByteCount::try_from(100)?))
            .build()?;
        db.live()
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
            .project_id(pfr.project_id.clone())
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
        db.live()
            .insert::<PhysicalTableDataVersionDB>(&ptdv)
            .await?;
        let pointer = LogicalTableDataVersionPointerDB::build_event(
            ltdv.project_id.clone(),
            &ltdv.id,
            LogicalPointerStatus::Bound,
            Some(ptdv.id.clone()),
            AtTime::try_from(pfr.triggered_on)?,
            ltdv.triggered_by_id,
            LogicalPointerReason::InitialDispatch,
        )?;
        db.live()
            .insert::<LogicalTableDataVersionPointerDB>(&pointer)
            .await?;
        Ok(ptdv)
    }

    /// Seed a publisher (no inputs) with a terminated PFR, stamped
    /// metrics, and a bound PTDV carrying row_count + data_bytes. The
    /// LTDV's view-resolved magnitudes will reflect the PTDV.
    async fn seed_publisher_with_ltdv(
        db: &TdPool,
        coll_name: &str,
        fn_name: &str,
    ) -> Result<
        (
            td_objects::dxo::collection::CollectionDB,
            LogicalFunctionRunDB,
            LogicalTableDataVersionDB,
        ),
        TdError,
    > {
        let coll = db
            .seed()
            .collection(
                &CollectionName::try_from(coll_name)?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let func = make_publisher_function(db, &coll, fn_name).await?;
        let exec = db.seed().execution(&func).await?;
        let trx = db
            .seed()
            .transaction(&exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let pfr = db
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

    // ----- /inflight ---------------------------------------------------

    #[td_test::test(db)]
    async fn test_inflight_empty_table(db: TdPool) -> Result<(), TdError> {
        let req = InflightRequest {
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let mut conn = db.acquire().await.unwrap();
        let rows = conn.metrics().inflight(&req).await?;
        assert!(rows.is_empty(), "no runs yet, got {rows:?}");
        Ok(())
    }

    // ----- /metrics nodes ----------------------------------------------

    #[td_test::test(db)]
    async fn test_nodes_empty_window(db: TdPool) -> Result<(), TdError> {
        let req = empty_request(Dim::Collection);
        let mut conn = db.acquire().await.unwrap();
        let nodes = conn.metrics().metrics_nodes(&req).await?;
        assert!(nodes.is_empty(), "empty DB → empty nodes, got {nodes:?}");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_nodes_publisher_out_magnitudes(db: TdPool) -> Result<(), TdError> {
        let (coll, _lfr, _ltdv) = seed_publisher_with_ltdv(&db, "pub_coll", "pub_fn").await?;

        let req = empty_request(Dim::Collection);
        let mut conn = db.acquire().await.unwrap();
        let nodes = conn.metrics().metrics_nodes(&req).await?;

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

    // ----- /metrics edges ----------------------------------------------

    #[td_test::test(db)]
    async fn test_edges_empty_window(db: TdPool) -> Result<(), TdError> {
        let req = empty_request(Dim::Collection);
        let mut conn = db.acquire().await.unwrap();
        let edges = conn.metrics().metrics_edges(&req).await?;
        assert!(edges.is_empty(), "empty DB → empty edges, got {edges:?}");
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_edges_synthetic_for_publisher(db: TdPool) -> Result<(), TdError> {
        let (coll, _, _ltdv) = seed_publisher_with_ltdv(&db, "pub_synth", "pub_synth_fn").await?;

        let req = empty_request(Dim::Collection);
        let mut conn = db.acquire().await.unwrap();
        let edges = conn.metrics().metrics_edges(&req).await?;

        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!(e.source_kind, NodeKind::ExternalSource);
        assert!(
            e.source.is_none(),
            "synth-source edge has source = None; the synthetic-kind tag is the discriminator"
        );
        assert_eq!(e.target_kind, NodeKind::Dim);
        assert_eq!(e.target, Some(coll.id.to_string()));
        assert_eq!(e.fn_runs, 1);
        assert_eq!(e.tables, 1, "publisher minted one output LTDV");
        // Source-LTDV attribution: row_count = LTDV.row_count = 10 (from PTDV via view).
        assert_eq!(*e.row_count.as_ref(), 10);
        // data_bytes = LTDV.data_bytes = NULL until the worker plumbs
        // it through PTDV.data_bytes; for the seed path it's None →
        // COALESCE'd to 0.
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_edges_real_edge_source_ltdv_attribution(db: TdPool) -> Result<(), TdError> {
        // Producer LFR (publisher) creates an output LTDV; consumer LFR
        // references that LTDV via an LFRI row. The real edge's
        // magnitudes come from the SOURCE LTDV (producer's PTDV.row_count),
        // not from the consumer's own output.

        let (producer_coll, _, producer_ltdv) =
            seed_publisher_with_ltdv(&db, "real_prod", "real_prod_fn").await?;

        let consumer_coll = db
            .seed()
            .collection(
                &CollectionName::try_from("real_cons")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let consumer_fn = make_publisher_function(&db, &consumer_coll, "real_cons_fn").await?;
        let consumer_exec = db.seed().execution(&consumer_fn).await?;
        let consumer_trx = db
            .seed()
            .transaction(&consumer_exec, &TransactionKey::try_from("ANY")?)
            .await?;
        let consumer_pfr = db
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
        db.live()
            .insert::<LogicalFunctionRunInputDB>(&input)
            .await?;

        let req = empty_request(Dim::Collection);
        let mut conn = db.acquire().await.unwrap();
        let edges = conn.metrics().metrics_edges(&req).await?;

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
        Ok(())
    }
}
