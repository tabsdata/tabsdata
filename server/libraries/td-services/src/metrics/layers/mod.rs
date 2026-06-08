//
// Copyright 2026 Tabs Data Inc.
//

//! `/metrics` post-passes: Quarter/Year roll-up from stored Month,
//! then gap-fill to a dense `buckets × dim_keys × statuses` shape.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use td_common::bucket::{WindowUnit as BucketUnit, floor_unit_ms};
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::metrics::{
    EdgeRow, InflightRequest, InflightResponse, MetricsRequest, MetricsResponse, NodeKind, NodeRow,
};
use td_objects::dxo::query_filters::{
    MAX_GAP_FILLED_ROWS, TimeWindowError, WindowUnit, expected_buckets,
};
use td_objects::types::basic::{
    ByteCount, CollectionId, CollectionName, EpochMs, GroupId, GroupName, LogicalFunctionRunStatus,
    RowCount, TableVersionCount,
};
use td_queries::MetricsEntry;
use td_tower::extractors::{MutRef, Ref};

pub async fn build_inflight_response(
    req: Ref<InflightRequest>,
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<InflightResponse, TdError> {
    let rows = conn.metrics().inflight(&req).await?;
    Ok(InflightResponse::builder().rows(rows).build()?)
}

pub async fn build_metrics_response(
    req: Ref<MetricsRequest>,
    mut conn: MutRef<DynamicConnection<'static>>,
) -> Result<MetricsResponse, TdError> {
    let raw_nodes = conn.metrics().metrics_nodes(&req).await?;
    let raw_edges = conn.metrics().metrics_edges(&req).await?;
    let nodes = fold_to_coarse(raw_nodes, req.unit)?;
    let edges = fold_to_coarse(raw_edges, req.unit)?;
    let buckets = expected_buckets(req.window.start_ms, req.window.end_ms, req.unit);
    let statuses = resolve_status_axis(&req.statuses);
    let nodes = gap_fill(nodes, &buckets, &statuses)?;
    let edges = gap_fill(edges, &buckets, &statuses)?;
    Ok(MetricsResponse::builder()
        .dim(req.dim)
        .unit(req.unit)
        .start_ms(req.window.start_ms)
        .end_ms(req.window.end_ms)
        .nodes(nodes)
        .edges(edges)
        .build()?)
}

/// Status axis for gap-fill: the request filter intersected with
/// ENDED variants (the SQL filters in-flight LFRs via `ended_on
/// IS NOT NULL`, so they cannot appear in `/metrics`). Empty filter →
/// borrow the static `ENDED` slice — zero-alloc on UI-polled defaults.
fn resolve_status_axis(
    filter: &[LogicalFunctionRunStatus],
) -> Cow<'static, [LogicalFunctionRunStatus]> {
    if filter.is_empty() {
        Cow::Borrowed(LogicalFunctionRunStatus::ENDED)
    } else {
        Cow::Owned(filter.iter().filter(|s| s.is_ended()).copied().collect())
    }
}

/// Quarter/Year are stored as Month rows in SQL; this trait lets one
/// folder coalesce them up to the coarser grain.
trait FoldRow: Sized {
    type Key: Eq + std::hash::Hash;
    fn bucket(&self) -> EpochMs;
    fn set_bucket(&mut self, b: EpochMs);
    fn key(&self, coarse_ms: i64) -> Self::Key;
    fn merge(&mut self, other: Self) -> Result<(), TdError>;
}

fn fold_to_coarse<R: FoldRow>(rows: Vec<R>, unit: WindowUnit) -> Result<Vec<R>, TdError> {
    if !matches!(unit, WindowUnit::Quarter | WindowUnit::Year) {
        return Ok(rows);
    }
    let unit: BucketUnit = unit.into();
    let mut acc: HashMap<R::Key, R> = HashMap::new();
    for row in rows {
        let coarse = floor_unit_ms(unit, *row.bucket());
        let key = row.key(coarse);
        match acc.get_mut(&key) {
            Some(existing) => existing.merge(row)?,
            None => {
                let mut moved = row;
                moved.set_bucket(EpochMs::try_from(coarse)?);
                acc.insert(key, moved);
            }
        }
    }
    Ok(acc.into_values().collect())
}

impl FoldRow for NodeRow {
    type Key = (
        i64,
        NodeKind,
        Option<CollectionId>,
        Option<GroupId>,
        LogicalFunctionRunStatus,
    );

    fn bucket(&self) -> EpochMs {
        self.bucket
    }
    fn set_bucket(&mut self, b: EpochMs) {
        self.bucket = b;
    }
    fn key(&self, coarse_ms: i64) -> Self::Key {
        (
            coarse_ms,
            self.kind,
            self.collection_id,
            self.group_id,
            self.status,
        )
    }
    fn merge(&mut self, other: Self) -> Result<(), TdError> {
        self.fn_runs += other.fn_runs;
        self.transactions += other.transactions;
        self.table_versions_out =
            TableVersionCount::try_from(*self.table_versions_out + *other.table_versions_out)?;
        self.row_count_out = RowCount::try_from(*self.row_count_out + *other.row_count_out)?;
        self.data_bytes_out = ByteCount::try_from(*self.data_bytes_out + *other.data_bytes_out)?;
        self.table_versions_in =
            TableVersionCount::try_from(*self.table_versions_in + *other.table_versions_in)?;
        self.row_count_in = RowCount::try_from(*self.row_count_in + *other.row_count_in)?;
        self.data_bytes_in = ByteCount::try_from(*self.data_bytes_in + *other.data_bytes_in)?;
        Ok(())
    }
}

impl FoldRow for EdgeRow {
    type Key = (
        i64,
        NodeKind,
        Option<String>,
        NodeKind,
        Option<String>,
        LogicalFunctionRunStatus,
    );

    fn bucket(&self) -> EpochMs {
        self.bucket
    }
    fn set_bucket(&mut self, b: EpochMs) {
        self.bucket = b;
    }
    fn key(&self, coarse_ms: i64) -> Self::Key {
        (
            coarse_ms,
            self.source_kind,
            self.source.clone(),
            self.target_kind,
            self.target.clone(),
            self.status,
        )
    }
    fn merge(&mut self, other: Self) -> Result<(), TdError> {
        self.fn_runs += other.fn_runs;
        self.tables += other.tables;
        self.row_count = RowCount::try_from(*self.row_count + *other.row_count)?;
        self.data_bytes = ByteCount::try_from(*self.data_bytes + *other.data_bytes)?;
        Ok(())
    }
}

/// A row whose bucket axis can be enumerated and whose missing slots
/// can be minted from `(dim_key, status)` carrying zero magnitudes —
/// the Rust analogue of `LEFT JOIN calendar_dim CROSS JOIN observed`.
trait GapFillRow: Sized {
    type DimKey: Eq + std::hash::Hash + Clone;
    fn bucket(&self) -> EpochMs;
    fn dim_key(&self) -> Self::DimKey;
    fn status(&self) -> LogicalFunctionRunStatus;
    fn zero(
        bucket: EpochMs,
        dim_key: &Self::DimKey,
        status: LogicalFunctionRunStatus,
    ) -> Result<Self, TdError>;
}

/// `dim_keys` includes any synthetic-kind singletons present in the
/// input (ExternalSource / ExternalDestination / Unused each
/// contribute one dim_key when observed). These count toward the
/// `MAX_GAP_FILLED_ROWS` cap alongside real Dim keys — three extra
/// singletons can flip a workspace tuned near the cap into a
/// `TooManyGapFilledRows` error. If that happens, the error's `dims`
/// field counts the synthetic singletons too; narrow the window or
/// dim filter rather than expecting the synthetic kinds to be free.
fn gap_fill<R: GapFillRow>(
    rows: Vec<R>,
    buckets: &[i64],
    statuses: &[LogicalFunctionRunStatus],
) -> Result<Vec<R>, TdError> {
    let dim_keys: HashSet<R::DimKey> = rows.iter().map(|r| r.dim_key()).collect();
    if dim_keys.is_empty() || statuses.is_empty() {
        return Ok(rows);
    }
    let n = buckets.len() * dim_keys.len() * statuses.len();
    if n > MAX_GAP_FILLED_ROWS {
        return Err(TimeWindowError::TooManyGapFilledRows {
            rows: n,
            buckets: buckets.len(),
            dims: dim_keys.len(),
            statuses: statuses.len(),
            cap: MAX_GAP_FILLED_ROWS,
        }
        .into());
    }

    let mut by_key: HashMap<(i64, R::DimKey, LogicalFunctionRunStatus), R> =
        HashMap::with_capacity(rows.len());
    for r in rows {
        by_key.insert((*r.bucket(), r.dim_key(), r.status()), r);
    }

    // Inner order is HashSet iteration order; chart consumers
    // group-by-bucket and don't depend on it.
    let mut out: Vec<R> = Vec::with_capacity(n);
    for &b in buckets.iter().rev() {
        let bucket = EpochMs::try_from(b)?;
        for k in &dim_keys {
            for s in statuses {
                let key = (b, k.clone(), *s);
                let row = match by_key.remove(&key) {
                    Some(r) => r,
                    None => R::zero(bucket, k, *s)?,
                };
                out.push(row);
            }
        }
    }
    Ok(out)
}

impl GapFillRow for NodeRow {
    // Names are functionally dependent on ids so HashSet cardinality is
    // unchanged; carrying them in the key lets `zero()` reconstruct
    // them for synthesised buckets without a separate id→name side-map.
    // Synthetic rows have None ids and a singleton dim_key per `kind` —
    // gap-fill spans them along buckets × statuses only.
    type DimKey = (
        NodeKind,
        Option<CollectionId>,
        Option<GroupId>,
        Option<CollectionName>,
        Option<GroupName>,
    );

    fn bucket(&self) -> EpochMs {
        self.bucket
    }
    fn dim_key(&self) -> Self::DimKey {
        (
            self.kind,
            self.collection_id,
            self.group_id,
            self.collection_name.clone(),
            self.group_name.clone(),
        )
    }
    fn status(&self) -> LogicalFunctionRunStatus {
        self.status
    }
    fn zero(
        bucket: EpochMs,
        dim_key: &Self::DimKey,
        status: LogicalFunctionRunStatus,
    ) -> Result<Self, TdError> {
        Ok(NodeRow {
            bucket,
            kind: dim_key.0,
            collection_id: dim_key.1,
            collection_name: dim_key.3.clone(),
            group_id: dim_key.2,
            group_name: dim_key.4.clone(),
            status,
            fn_runs: 0,
            transactions: 0,
            table_versions_out: TableVersionCount::default(),
            row_count_out: RowCount::default(),
            data_bytes_out: ByteCount::default(),
            table_versions_in: TableVersionCount::default(),
            row_count_in: RowCount::default(),
            data_bytes_in: ByteCount::default(),
        })
    }
}

impl GapFillRow for EdgeRow {
    type DimKey = (
        NodeKind,
        Option<String>,
        Option<String>,
        NodeKind,
        Option<String>,
        Option<String>,
    );

    fn bucket(&self) -> EpochMs {
        self.bucket
    }
    fn dim_key(&self) -> Self::DimKey {
        (
            self.source_kind,
            self.source.clone(),
            self.source_name.clone(),
            self.target_kind,
            self.target.clone(),
            self.target_name.clone(),
        )
    }
    fn status(&self) -> LogicalFunctionRunStatus {
        self.status
    }
    fn zero(
        bucket: EpochMs,
        dim_key: &Self::DimKey,
        status: LogicalFunctionRunStatus,
    ) -> Result<Self, TdError> {
        Ok(EdgeRow {
            bucket,
            source_kind: dim_key.0,
            source: dim_key.1.clone(),
            source_name: dim_key.2.clone(),
            target_kind: dim_key.3,
            target: dim_key.4.clone(),
            target_name: dim_key.5.clone(),
            status,
            fn_runs: 0,
            tables: 0,
            row_count: RowCount::default(),
            data_bytes: ByteCount::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::types::basic::{CollectionId, GroupId, LogicalFunctionRunStatus};

    // Monthly UTC ms for 2026 — first of each month at 00:00:00 UTC.
    const JAN_2026: i64 = 1_767_225_600_000;
    const FEB_2026: i64 = 1_769_904_000_000;
    const MAR_2026: i64 = 1_772_323_200_000;
    const APR_2026: i64 = 1_775_001_600_000;
    const JUL_2026: i64 = 1_782_864_000_000;
    const NOV_2026: i64 = 1_793_491_200_000;
    const Q1_2026: i64 = JAN_2026;
    const Q2_2026: i64 = APR_2026;
    const Y_2026: i64 = JAN_2026;

    /// `CollectionId::default()` mints a new id per call; tests need
    /// a stable id so fold keys collapse.
    fn fixed_collection_id() -> CollectionId {
        CollectionId::try_from("00000000000000000000000C00").unwrap()
    }

    fn node(bucket_ms: i64, status: LogicalFunctionRunStatus, fn_runs: i64) -> NodeRow {
        NodeRow {
            bucket: EpochMs::try_from(bucket_ms).unwrap(),
            kind: NodeKind::Dim,
            collection_id: Some(fixed_collection_id()),
            collection_name: Some(CollectionName::try_from("test_coll").unwrap()),
            group_id: Some(GroupId::default_group()),
            group_name: Some(GroupName::default_group()),
            status,
            fn_runs,
            transactions: fn_runs,
            table_versions_out: TableVersionCount::try_from(fn_runs).unwrap(),
            row_count_out: RowCount::try_from(fn_runs * 10).unwrap(),
            data_bytes_out: ByteCount::try_from(fn_runs * 100).unwrap(),
            table_versions_in: TableVersionCount::try_from(0).unwrap(),
            row_count_in: RowCount::try_from(0).unwrap(),
            data_bytes_in: ByteCount::try_from(0).unwrap(),
        }
    }

    fn synth_source_node(
        bucket_ms: i64,
        status: LogicalFunctionRunStatus,
        fn_runs: i64,
    ) -> NodeRow {
        NodeRow {
            bucket: EpochMs::try_from(bucket_ms).unwrap(),
            kind: NodeKind::ExternalSource,
            collection_id: None,
            collection_name: None,
            group_id: None,
            group_name: None,
            status,
            fn_runs,
            transactions: fn_runs,
            table_versions_out: TableVersionCount::try_from(fn_runs).unwrap(),
            row_count_out: RowCount::try_from(fn_runs * 10).unwrap(),
            data_bytes_out: ByteCount::try_from(fn_runs * 100).unwrap(),
            table_versions_in: TableVersionCount::try_from(0).unwrap(),
            row_count_in: RowCount::try_from(0).unwrap(),
            data_bytes_in: ByteCount::try_from(0).unwrap(),
        }
    }

    fn edge(bucket_ms: i64, status: LogicalFunctionRunStatus, fn_runs: i64) -> EdgeRow {
        EdgeRow {
            bucket: EpochMs::try_from(bucket_ms).unwrap(),
            source_kind: NodeKind::ExternalSource,
            source: None,
            source_name: None,
            target_kind: NodeKind::Dim,
            target: Some("tgt".to_string()),
            target_name: Some("tgt".to_string()),
            status,
            fn_runs,
            tables: fn_runs,
            row_count: RowCount::try_from(fn_runs * 10).unwrap(),
            data_bytes: ByteCount::try_from(fn_runs * 100).unwrap(),
        }
    }

    #[test]
    fn fold_to_coarse_noop_for_hour_day_week_month() {
        let rows = vec![node(0, LogicalFunctionRunStatus::Done, 1)];
        let out = fold_to_coarse(rows, WindowUnit::Day).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn fold_to_coarse_quarter_folds_three_months() {
        let rows = vec![
            node(JAN_2026, LogicalFunctionRunStatus::Done, 1),
            node(FEB_2026, LogicalFunctionRunStatus::Done, 2),
            node(MAR_2026, LogicalFunctionRunStatus::Done, 4),
        ];
        let out = fold_to_coarse(rows, WindowUnit::Quarter).unwrap();
        assert_eq!(out.len(), 1, "expected one Q1 row, got {out:?}");
        assert_eq!(*out[0].bucket, Q1_2026);
        assert_eq!(out[0].fn_runs, 7);
        assert_eq!(*out[0].row_count_out, 70);
    }

    #[test]
    fn fold_to_coarse_quarter_keeps_statuses_separate() {
        let rows = vec![
            node(JAN_2026, LogicalFunctionRunStatus::Done, 1),
            node(JAN_2026, LogicalFunctionRunStatus::Failed, 2),
            node(FEB_2026, LogicalFunctionRunStatus::Done, 4),
        ];
        let out = fold_to_coarse(rows, WindowUnit::Quarter).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn fold_to_coarse_edges_quarter_folds_three_months() {
        let rows = vec![
            edge(JAN_2026, LogicalFunctionRunStatus::Done, 1),
            edge(FEB_2026, LogicalFunctionRunStatus::Done, 2),
            edge(MAR_2026, LogicalFunctionRunStatus::Done, 4),
        ];
        let out = fold_to_coarse(rows, WindowUnit::Quarter).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(*out[0].bucket, Q1_2026);
        assert_eq!(out[0].fn_runs, 7);
    }

    /// A window that crosses a quarter boundary must produce two rows,
    /// not one — Feb (Q1) and Apr (Q2) belong to distinct quarters.
    #[test]
    fn fold_to_coarse_quarter_does_not_fold_across_quarters() {
        let rows = vec![
            node(FEB_2026, LogicalFunctionRunStatus::Done, 1),
            node(APR_2026, LogicalFunctionRunStatus::Done, 2),
        ];
        let mut out = fold_to_coarse(rows, WindowUnit::Quarter).unwrap();
        out.sort_by_key(|r| *r.bucket);
        assert_eq!(out.len(), 2);
        assert_eq!(*out[0].bucket, Q1_2026);
        assert_eq!(out[0].fn_runs, 1);
        assert_eq!(*out[1].bucket, Q2_2026);
        assert_eq!(out[1].fn_runs, 2);
    }

    /// Year folding pulls rows from any quarter (Mar Q1, Jul Q3, Nov Q4)
    /// into a single row keyed at Jan 2026.
    #[test]
    fn fold_to_coarse_year_folds_across_quarters() {
        let rows = vec![
            node(MAR_2026, LogicalFunctionRunStatus::Done, 1),
            node(JUL_2026, LogicalFunctionRunStatus::Done, 2),
            node(NOV_2026, LogicalFunctionRunStatus::Done, 4),
        ];
        let out = fold_to_coarse(rows, WindowUnit::Year).unwrap();
        assert_eq!(out.len(), 1, "expected one Y2026 row, got {out:?}");
        assert_eq!(*out[0].bucket, Y_2026);
        assert_eq!(out[0].fn_runs, 7);
        assert_eq!(*out[0].row_count_out, 70);
    }

    /// Same year-folding behavior for the edge axis.
    #[test]
    fn fold_to_coarse_edges_year_folds_across_quarters() {
        let rows = vec![
            edge(MAR_2026, LogicalFunctionRunStatus::Done, 1),
            edge(JUL_2026, LogicalFunctionRunStatus::Done, 2),
            edge(NOV_2026, LogicalFunctionRunStatus::Done, 4),
        ];
        let out = fold_to_coarse(rows, WindowUnit::Year).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(*out[0].bucket, Y_2026);
        assert_eq!(out[0].fn_runs, 7);
    }

    /// Two years' worth of monthly rows fold into two separate yearly
    /// rows, not one — year boundary is respected.
    #[test]
    fn fold_to_coarse_year_does_not_fold_across_years() {
        const NOV_2025: i64 = 1_761_955_200_000;
        const Y_2025: i64 = 1_735_689_600_000;
        let rows = vec![
            node(NOV_2025, LogicalFunctionRunStatus::Done, 1),
            node(FEB_2026, LogicalFunctionRunStatus::Done, 2),
        ];
        let mut out = fold_to_coarse(rows, WindowUnit::Year).unwrap();
        out.sort_by_key(|r| *r.bucket);
        assert_eq!(out.len(), 2);
        assert_eq!(*out[0].bucket, Y_2025);
        assert_eq!(out[0].fn_runs, 1);
        assert_eq!(*out[1].bucket, Y_2026);
        assert_eq!(out[1].fn_runs, 2);
    }

    // -------- Gap-fill ------------------------------------------------------

    fn day_buckets(start: i64, span_days: i64) -> Vec<i64> {
        let end = start + span_days * 86_400_000;
        expected_buckets(start, end, WindowUnit::Day)
    }

    #[test]
    fn gap_fill_inserts_missing_buckets() {
        let rows = vec![node(JAN_2026, LogicalFunctionRunStatus::Done, 5)];
        let buckets = day_buckets(JAN_2026, 3);
        let out = gap_fill(rows, &buckets, &[LogicalFunctionRunStatus::Done]).unwrap();
        assert_eq!(out.len(), 3);
        let totals: Vec<i64> = out.iter().map(|r| r.fn_runs).collect();
        assert!(totals.contains(&5));
        assert_eq!(totals.iter().filter(|&&n| n == 0).count(), 2);
    }

    #[test]
    fn gap_fill_expands_status_axis_when_filter_empty() {
        // Empty filter → all 6 ENDED variants populate the axis.
        let rows = vec![node(JAN_2026, LogicalFunctionRunStatus::Done, 5)];
        let buckets = day_buckets(JAN_2026, 1);
        let out = gap_fill(rows, &buckets, LogicalFunctionRunStatus::ENDED).unwrap();
        assert_eq!(out.len(), 6);
        assert_eq!(out.iter().filter(|r| r.fn_runs == 5).count(), 1);
        assert_eq!(out.iter().filter(|r| r.fn_runs == 0).count(), 5);
    }

    #[test]
    fn gap_fill_empty_input_stays_empty() {
        // No observed dim keys → no cross-product to fill.
        let rows: Vec<NodeRow> = vec![];
        let buckets = day_buckets(JAN_2026, 30);
        let out = gap_fill(rows, &buckets, LogicalFunctionRunStatus::ENDED).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn gap_fill_runs_after_quarter_fold() {
        let rows = vec![
            node(JAN_2026, LogicalFunctionRunStatus::Done, 1),
            node(FEB_2026, LogicalFunctionRunStatus::Done, 2),
        ];
        let folded = fold_to_coarse(rows, WindowUnit::Quarter).unwrap();
        assert_eq!(folded.len(), 1);
        const JAN_2027: i64 = 1_798_761_600_000;
        let buckets = expected_buckets(JAN_2026, JAN_2027, WindowUnit::Quarter);
        let out = gap_fill(folded, &buckets, &[LogicalFunctionRunStatus::Done]).unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out.iter().filter(|r| r.fn_runs == 3).count(), 1);
        assert_eq!(out.iter().filter(|r| r.fn_runs == 0).count(), 3);
    }

    /// Synthetic node rows have None ids — gap-fill should treat them
    /// as a singleton dim_key per kind and span them across buckets ×
    /// statuses without mixing with real `Dim` rows.
    #[test]
    fn gap_fill_synthetic_kind_singleton() {
        let rows = vec![
            node(JAN_2026, LogicalFunctionRunStatus::Done, 3),
            synth_source_node(JAN_2026, LogicalFunctionRunStatus::Done, 7),
        ];
        let buckets = day_buckets(JAN_2026, 2);
        let out = gap_fill(rows, &buckets, &[LogicalFunctionRunStatus::Done]).unwrap();
        // 2 buckets × 2 dim_keys (Dim + ExternalSource) × 1 status = 4 rows.
        assert_eq!(out.len(), 4);
        let dim_rows: Vec<_> = out.iter().filter(|r| r.kind == NodeKind::Dim).collect();
        let synth_rows: Vec<_> = out
            .iter()
            .filter(|r| r.kind == NodeKind::ExternalSource)
            .collect();
        assert_eq!(dim_rows.len(), 2);
        assert_eq!(synth_rows.len(), 2);
        assert_eq!(dim_rows.iter().filter(|r| r.fn_runs == 3).count(), 1);
        assert_eq!(synth_rows.iter().filter(|r| r.fn_runs == 7).count(), 1);
    }

    #[test]
    fn gap_fill_rejects_over_cap() {
        // 90 buckets × 1000 dims × 6 statuses = 540k, well over the 50k cap.
        let rows: Vec<NodeRow> = (0..1000)
            .map(|_| NodeRow {
                bucket: EpochMs::try_from(JAN_2026).unwrap(),
                kind: NodeKind::Dim,
                collection_id: Some(CollectionId::default()),
                collection_name: Some(CollectionName::try_from("test_coll").unwrap()),
                group_id: Some(GroupId::default_group()),
                group_name: Some(GroupName::default_group()),
                status: LogicalFunctionRunStatus::Done,
                fn_runs: 1,
                transactions: 1,
                table_versions_out: TableVersionCount::default(),
                row_count_out: RowCount::default(),
                data_bytes_out: ByteCount::default(),
                table_versions_in: TableVersionCount::default(),
                row_count_in: RowCount::default(),
                data_bytes_in: ByteCount::default(),
            })
            .collect();
        let buckets = day_buckets(JAN_2026, 90);
        let err = gap_fill(rows, &buckets, LogicalFunctionRunStatus::ENDED).unwrap_err();
        assert!(err.to_string().contains("exceeds cap"), "got: {err}");
    }
}
