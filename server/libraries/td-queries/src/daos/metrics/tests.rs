//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use crate::test_utils::SeedEntry;
use ta_objects::and;
use td_common::datetime::Timestamp;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDeleteDB};
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputInsertDB, LogicalTableDataVersionDB,
};
use td_objects::dxo::metrics::{Dimension, InflightRequest, MetricsRequest, NodeKind};
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, PhysicalFunctionRunInsertDB, PhysicalFunctionRunUpdateDB,
};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionInsertDB,
};
use td_objects::dxo::project::{ActiveProjectDB, ProjectDeleteDB};
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow, WindowUnit};
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, BasePath, BundleId, ByteCount, CollectionName, Decorator, DependencyPos, Ended,
    EpochMs, HasData, InputIdx, LogicalFunctionRunInputId, LogicalFunctionRunStatus,
    PhysicalFunctionRunId, PhysicalFunctionRunStatus, PhysicalTableDataVersionId, ProjectId,
    ProjectName, ReuseFrozen, RowCount, TableFunctionParamPos, TableNameDto, TableVersionCount,
    TransactionKey, TriggeredOn, UserId, VersionPos,
};
use td_objects::types::visible_projects::VisibleProjects;
use td_security::ENCODED_ID_SYSTEM;

const WINDOW_DAY_START: i64 = 1_700_000_000_000;
const WINDOW_DAY_END: i64 = WINDOW_DAY_START + 86_400_000;
const IN_WINDOW: i64 = WINDOW_DAY_START + 3_600_000;

fn in_window() -> Result<AtTime, TdError> {
    Ok(AtTime::from(Timestamp::try_from(IN_WINDOW)?))
}

fn system_user() -> Result<UserId, TdError> {
    UserId::try_from(ENCODED_ID_SYSTEM)
}

fn empty_request(dim: Dimension) -> MetricsRequest {
    MetricsRequest {
        dim,
        window: TimeWindow::new(WINDOW_DAY_START, WINDOW_DAY_END),
        unit: WindowUnit::Day,
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    }
}

fn inflight_request() -> InflightRequest {
    InflightRequest {
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: vec![],
    }
}

async fn make_function(
    db: &TdPool,
    collection: &ActiveCollectionDB,
    name: &str,
    decorator: Decorator,
    tables: &[&str],
) -> Result<FunctionDB, TdError> {
    let tables = tables
        .iter()
        .map(|t| TableNameDto::try_from(*t))
        .collect::<Result<Vec<_>, _>>()?;
    let create = FunctionRegister {
        name: name.try_into()?,
        description: "test fn".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "snippet".try_into()?,
        decorator,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: Some(tables),
        table_types: None,
        runtime_values: "rv".try_into()?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    db.scoped(collection.project_id)
        .seed()
        .function(collection, &create)
        .await
}

async fn make_publisher_function(
    db: &TdPool,
    collection: &ActiveCollectionDB,
    name: &str,
) -> Result<FunctionDB, TdError> {
    make_function(db, collection, name, Decorator::Publisher, &["out"]).await
}

async fn stamp_buckets_on_pfr(db: &TdPool, pfr: &PhysicalFunctionRunDB) -> Result<(), TdError> {
    stamp_buckets_and_metrics_on_pfr(db, pfr, 1, 10, 100).await
}

/// The attempt's metric columns hold the worker's full totals, private
/// tables included; `/metrics` excludes private tables query-side, so tests
/// stamp totals that differ from the expected response.
async fn stamp_buckets_and_metrics_on_pfr(
    db: &TdPool,
    pfr: &PhysicalFunctionRunDB,
    tables: i64,
    rows: i64,
    bytes: i64,
) -> Result<(), TdError> {
    let ended_on: Ended<AtTime> = in_window()?.into();
    let bucket = EpochMs::try_from(IN_WINDOW)?;
    let update = PhysicalFunctionRunUpdateDB {
        dispatched_on: None,
        started_on: None,
        ended_on: Some(ended_on),
        status: PhysicalFunctionRunStatus::Done,
        ended_on_hour_ms: Some(bucket.into()),
        ended_on_day_ms: Some(bucket.into()),
        ended_on_week_ms: Some(bucket.into()),
        ended_on_month_ms: Some(bucket.into()),
        metric_table_versions: Some(TableVersionCount::try_from(tables)?),
        metric_rows: Some(RowCount::try_from(rows)?),
        metric_data_bytes: Some(ByteCount::try_from(bytes)?),
    };
    db.scoped(pfr.project_id)
        .scd2()
        .update(&update, &pfr.base.id)
        .await?;
    Ok(())
}

/// Mint a PTDV linked to the run's attempt, the version's current
/// materialization, carrying `row_count` / `data_bytes`.
async fn bind_ptdv_with_magnitudes(
    db: &TdPool,
    ltdv: &LogicalTableDataVersionDB,
    table: &TableDB,
    pfr: &PhysicalFunctionRunDB,
    row_count: i64,
    data_bytes: i64,
) -> Result<PhysicalTableDataVersionDB, TdError> {
    let ptdv = PhysicalTableDataVersionInsertDB {
        id: PhysicalTableDataVersionId::new(),
        collection_id: ltdv.base.collection_id,
        table_id: table.base.table_id,
        name: table.base.name.clone(),
        table_version_id: table.base.id,
        function_version_id: table.base.function_version_id.clone(),
        function_version_patch_id: None,
        execution_id: ltdv.base.execution_id.clone(),
        transaction_id: ltdv.base.transaction_id.clone(),
        physical_function_run_id: pfr.base.id.clone(),
        function_param_pos: table
            .base
            .function_param_pos
            .unwrap_or(TableFunctionParamPos::try_from(0)?),
        triggered_on: pfr.base.triggered_on,
        base_path: BasePath::default(),
        has_data: Some(HasData::TRUE),
        schema_changed: None,
        column_count: None,
        row_count: Some(RowCount::try_from(row_count)?),
        data_bytes: Some(ByteCount::try_from(data_bytes)?),
        schema_hash: None,
        with_data_table_data_version_id: None,
        logical_table_data_version_id: ltdv.base.id.clone(),
    };
    let scoped = db.scoped(pfr.project_id);
    (&scoped).scd2().insert(&ptdv).await?;
    Ok((&scoped).timeline().fetch_one(&ptdv.id).await?)
}

async fn seed_collection(
    db: &TdPool,
    project_id: ProjectId,
    name: &str,
) -> Result<ActiveCollectionDB, TdError> {
    db.scoped(project_id)
        .seed()
        .collection(&CollectionName::try_from(name)?, &system_user()?)
        .await
}

async fn soft_delete_collection(db: &TdPool, coll: &ActiveCollectionDB) -> Result<(), TdError> {
    let del = CollectionDeleteDB {
        name: CollectionName::try_from(format!("deleted_{}", coll.id))?,
        modified_on: in_window()?.into(),
        modified_by_id: system_user()?.into(),
        name_when_deleted: coll.name.clone().into(),
    };
    db.scoped(coll.project_id).update(&del, &coll.id).await?;
    Ok(())
}

async fn soft_delete_project(db: &TdPool, project: &ActiveProjectDB) -> Result<(), TdError> {
    let del = ProjectDeleteDB {
        name: ProjectName::try_from(format!("deleted_{}", project.id))?,
        modified_on: in_window()?.into(),
        modified_by_id: system_user()?.into(),
        name_when_deleted: project.name.clone().into(),
    };
    db.scoped(project.id).update(&del, &project.id).await?;
    Ok(())
}

/// Seed an execution, transaction and attempt in `status` for the function;
/// buckets and metrics are not stamped.
async fn seed_run(
    db: &TdPool,
    coll: &ActiveCollectionDB,
    func: &FunctionDB,
    status: LogicalFunctionRunStatus,
) -> Result<(LogicalFunctionRunDB, PhysicalFunctionRunDB), TdError> {
    let scoped = db.scoped(coll.project_id);
    let exec = scoped.seed().execution(func).await?;
    let trx = scoped
        .seed()
        .transaction(&exec, &TransactionKey::try_from("ANY")?)
        .await?;
    let pfr = scoped
        .seed()
        .physical_function_run(coll, func, &exec, &trx, &status)
        .await?;
    let lfr: LogicalFunctionRunDB = db
        .fetch_one(and!(&pfr.base.execution_id, &pfr.base.function_id))
        .await?;
    Ok((lfr, pfr))
}

async fn seed_done_run(
    db: &TdPool,
    coll: &ActiveCollectionDB,
    func: &FunctionDB,
) -> Result<(LogicalFunctionRunDB, PhysicalFunctionRunDB), TdError> {
    seed_run(db, coll, func, LogicalFunctionRunStatus::Done).await
}

/// Make `consumer` consume `ltdv` as its first user dependency.
async fn link_input(
    db: &TdPool,
    consumer_coll: &ActiveCollectionDB,
    consumer: &LogicalFunctionRunDB,
    ltdv: &LogicalTableDataVersionDB,
) -> Result<(), TdError> {
    let input = LogicalFunctionRunInputInsertDB {
        id: LogicalFunctionRunInputId::default(),
        collection_id: consumer_coll.id,
        execution_id: consumer.base.execution_id.clone(),
        transaction_id: consumer.base.transaction_id.clone(),
        logical_function_run_id: consumer.base.id.clone(),
        requirement_logical_table_data_version_id: Some(ltdv.base.id.clone()),
        requirement_input_idx: Some(InputIdx::try_from(0)?),
        requirement_dependency_pos: Some(DependencyPos::try_from(0)?),
        requirement_version_pos: VersionPos::try_from(0)?,
    };
    db.scoped(consumer_coll.project_id).insert(&input).await?;
    Ok(())
}

/// A publisher with a Done attempt, stamped metrics and its output version
/// materialized with 10 rows / 100 bytes.
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
    let (lfr, pfr) = seed_done_run(db, &coll, &func).await?;
    stamp_buckets_on_pfr(db, &pfr).await?;
    let ltdvs: Vec<LogicalTableDataVersionDB> =
        db.timeline().fetch_all(&lfr.base.execution_id).await?;
    let ltdv = ltdvs
        .into_iter()
        .find(|t| t.base.producer_logical_function_run_id.as_ref() == Some(&lfr.base.id))
        .expect("publisher mints exactly one output LTDV");
    let tables: Vec<TableDB> = db.scd2().fetch_all::<TableDB>(&func.base.id).await?;
    let table = tables
        .iter()
        .find(|t| t.base.table_id == ltdv.base.table_id)
        .expect("LTDV's table_id must match an output table of the function");
    bind_ptdv_with_magnitudes(db, &ltdv, table, &pfr, 10, 100).await?;
    Ok((coll, lfr, ltdv))
}

/// Materialize every output version of the run: regular tables get
/// (10, 100), private tables (7, 77).
async fn bind_all_outputs(
    db: &TdPool,
    func: &FunctionDB,
    lfr: &LogicalFunctionRunDB,
    pfr: &PhysicalFunctionRunDB,
) -> Result<(), TdError> {
    let ltdvs: Vec<LogicalTableDataVersionDB> =
        db.timeline().fetch_all(&lfr.base.execution_id).await?;
    let tables: Vec<TableDB> = db.scd2().fetch_all::<TableDB>(&func.base.id).await?;
    for table in &tables {
        let ltdv = ltdvs
            .iter()
            .find(|l| l.base.table_id == table.base.table_id)
            .expect("one LTDV per output table");
        let (rows, bytes) = if *table.base.private {
            (7, 77)
        } else {
            (10, 100)
        };
        bind_ptdv_with_magnitudes(db, ltdv, table, pfr, rows, bytes).await?;
    }
    Ok(())
}

#[td_test::test(db)]
async fn test_inflight_empty_table(db: TdPool) -> Result<(), TdError> {
    let rows = db
        .metrics()
        .inflight(&inflight_request(), &VisibleProjects::All)
        .await?;
    assert!(rows.is_empty(), "no runs yet, got {rows:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_inflight_excludes_soft_deleted_collection(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = seed_collection(&db, p.id, "inflight_del").await?;
    let func = make_publisher_function(&db, &coll, "inflight_del_fn").await?;
    seed_run(&db, &coll, &func, LogicalFunctionRunStatus::Running).await?;

    let req = inflight_request();
    assert!(
        !db.metrics()
            .inflight(&req, &VisibleProjects::All)
            .await?
            .is_empty(),
        "a live collection's in-flight run must show"
    );

    soft_delete_collection(&db, &coll).await?;
    assert!(
        db.metrics()
            .inflight(&req, &VisibleProjects::All)
            .await?
            .is_empty(),
        "a soft-deleted collection's in-flight run must be excluded"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_nodes_empty_window(db: TdPool) -> Result<(), TdError> {
    let req = empty_request(Dimension::Collection);
    let nodes = db
        .metrics()
        .metrics_nodes(&req, &VisibleProjects::All)
        .await?;
    assert!(nodes.is_empty(), "empty DB → empty nodes, got {nodes:?}");
    Ok(())
}

#[td_test::test(db)]
async fn test_nodes_rejects_inverted_window(db: TdPool) -> Result<(), TdError> {
    let mut req = empty_request(Dimension::Collection);
    req.window = TimeWindow::new(WINDOW_DAY_END, WINDOW_DAY_START);
    assert!(
        db.metrics()
            .metrics_nodes(&req, &VisibleProjects::All)
            .await
            .is_err()
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_nodes_publisher_out_magnitudes(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (coll, _lfr, _ltdv) = seed_publisher_with_ltdv(&db, p.id, "pub_coll", "pub_fn").await?;

    let req = empty_request(Dimension::Collection);
    let nodes = db
        .metrics()
        .metrics_nodes(&req, &VisibleProjects::All)
        .await?;

    assert_eq!(
        nodes.len(),
        1,
        "one publisher → one node row, got {nodes:?}"
    );
    let n = &nodes[0];
    assert_eq!(n.collection_id.as_ref(), Some(&coll.id));
    assert_eq!(n.fn_runs, 1);
    assert_eq!(*n.table_versions_out, 1);
    assert_eq!(*n.row_count_out, 10);
    assert_eq!(*n.data_bytes_out, 100);
    assert_eq!(*n.table_versions_in, 0);
    assert_eq!(*n.row_count_in, 0);
    assert_eq!(*n.data_bytes_in, 0);
    Ok(())
}

#[td_test::test(db)]
async fn test_nodes_out_magnitudes_follow_the_current_materialization(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_coll, _lfr, ltdv) = seed_publisher_with_ltdv(&db, p.id, "void_coll", "void_fn").await?;

    let req = empty_request(Dimension::Collection);
    let before = db
        .metrics()
        .metrics_nodes(&req, &VisibleProjects::All)
        .await?;
    assert_eq!(*before[0].row_count_out, 10);
    assert_eq!(*before[0].data_bytes_out, 100);

    // A retry that produced nothing supersedes the materialization the
    // view was resolving, so its magnitudes stop counting.
    let current: PhysicalTableDataVersionDB = db
        .scoped(ltdv.project_id)
        .scd2()
        .fetch_one::<PhysicalTableDataVersionDB>(&ltdv.base.id)
        .await?;
    let producer: PhysicalFunctionRunDB = db
        .timeline()
        .fetch_one(&current.base.physical_function_run_id)
        .await?;
    let retried_on =
        TriggeredOn::try_from(AtTime::from(Timestamp::try_from(4_102_444_800_000_i64)?))?;
    let retry = PhysicalFunctionRunInsertDB {
        id: PhysicalFunctionRunId::new(),
        triggered_on: retried_on,
        ..producer.base
    };
    db.scoped(p.id).scd2().insert(&retry).await?;
    let empty = PhysicalTableDataVersionInsertDB {
        id: PhysicalTableDataVersionId::new(),
        physical_function_run_id: retry.id.clone(),
        triggered_on: retried_on,
        has_data: None,
        row_count: None,
        data_bytes: None,
        ..current.base
    };
    db.scoped(p.id).scd2().insert(&empty).await?;

    let after = db
        .metrics()
        .metrics_nodes(&req, &VisibleProjects::All)
        .await?;
    assert_eq!(
        after.len(),
        1,
        "the run still exists, only its output magnitudes are gone"
    );
    assert_eq!(*after[0].row_count_out, 0);
    assert_eq!(*after[0].data_bytes_out, 0);
    Ok(())
}

#[td_test::test(db)]
async fn test_node_output_tables_lists_distinct_output_table(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (coll, _lfr, ltdv) = seed_publisher_with_ltdv(&db, p.id, "lot_coll", "lot_fn").await?;

    let req = empty_request(Dimension::Collection);
    let rows = db
        .metrics()
        .metrics_node_output_tables(&req, &VisibleProjects::All)
        .await?;

    assert_eq!(
        rows.len(),
        1,
        "one publisher, one output table, got {rows:?}"
    );
    assert_eq!(rows[0].collection_id.as_ref(), Some(&coll.id));
    assert_eq!(rows[0].table_id, ltdv.base.table_id);
    assert_eq!(rows[0].status, LogicalFunctionRunStatus::Done);
    Ok(())
}

#[td_test::test(db)]
async fn test_edges_empty_window(db: TdPool) -> Result<(), TdError> {
    let req = empty_request(Dimension::Collection);
    let edges = db
        .metrics()
        .metrics_edges(&req, &VisibleProjects::All)
        .await?;
    assert!(edges.is_empty(), "empty DB → empty edges, got {edges:?}");
    Ok(())
}

/// An unconsumed publisher output yields an ingress edge
/// (ExternalSource → coll) and an unused edge (coll → Unused), both carrying
/// the version's magnitudes.
#[td_test::test(db)]
async fn test_edges_synthetic_for_publisher(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (coll, _, _ltdv) = seed_publisher_with_ltdv(&db, p.id, "pub_synth", "pub_synth_fn").await?;

    let req = empty_request(Dimension::Collection);
    let edges = db
        .metrics()
        .metrics_edges(&req, &VisibleProjects::All)
        .await?;

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
    assert_eq!(*ie.row_count, 10);

    let ue = unused[0];
    assert_eq!(ue.source_kind, NodeKind::Dim);
    assert_eq!(ue.source, Some(coll.id.to_string()));
    assert!(ue.target.is_none());
    assert_eq!(ue.fn_runs, 1);
    assert_eq!(ue.tables, 1);
    assert_eq!(*ue.row_count, 10);
    Ok(())
}

/// A real edge's magnitudes come from the consumed source version.
#[td_test::test(db)]
async fn test_edges_real_edge_source_ltdv_attribution(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (producer_coll, _, producer_ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "real_prod", "real_prod_fn").await?;

    let consumer_coll = seed_collection(&db, p.id, "real_cons").await?;
    let consumer_fn = make_publisher_function(&db, &consumer_coll, "real_cons_fn").await?;
    let (consumer_lfr, consumer_pfr) = seed_done_run(&db, &consumer_coll, &consumer_fn).await?;
    stamp_buckets_on_pfr(&db, &consumer_pfr).await?;
    link_input(&db, &consumer_coll, &consumer_lfr, &producer_ltdv).await?;

    let req = empty_request(Dimension::Collection);
    let edges = db
        .metrics()
        .metrics_edges(&req, &VisibleProjects::All)
        .await?;

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
    assert_eq!(*e.row_count, 10);

    let edge_tables = db
        .metrics()
        .metrics_edge_tables(&req, &VisibleProjects::All)
        .await?;
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
    assert_eq!(real_tbls[0].table_id, producer_ltdv.base.table_id);
    Ok(())
}

/// A consumer with no output versions is a topological subscriber: its
/// consumed input shows as an egress edge, and the producer's version is
/// consumed, so it is not unused.
#[td_test::test(db)]
async fn test_edges_egress_for_subscriber(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (producer_coll, _, producer_ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "egress_prod", "egress_prod_fn").await?;

    let consumer_coll = seed_collection(&db, p.id, "egress_cons").await?;
    let consumer_fn = make_function(
        &db,
        &consumer_coll,
        "egress_cons_fn",
        Decorator::Subscriber,
        &[],
    )
    .await?;
    let (consumer_lfr, consumer_pfr) = seed_done_run(&db, &consumer_coll, &consumer_fn).await?;
    stamp_buckets_on_pfr(&db, &consumer_pfr).await?;
    link_input(&db, &consumer_coll, &consumer_lfr, &producer_ltdv).await?;

    let req = empty_request(Dimension::Collection);
    let edges = db
        .metrics()
        .metrics_edges(&req, &VisibleProjects::All)
        .await?;

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
    assert_eq!(*e.row_count, 10);
    let unused: Vec<_> = edges
        .iter()
        .filter(|e| e.target_kind == NodeKind::Unused)
        .collect();
    assert!(
        unused.is_empty(),
        "consumed LTDV must not appear as unused, got {unused:?}"
    );
    let ingress: Vec<_> = edges
        .iter()
        .filter(|e| e.source_kind == NodeKind::ExternalSource)
        .collect();
    assert_eq!(ingress.len(), 1);
    assert_eq!(ingress[0].target, Some(producer_coll.id.to_string()));
    Ok(())
}

#[td_test::test(db)]
async fn test_private_output_excluded_from_nodes_and_edges(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = seed_collection(&db, p.id, "priv_out").await?;
    let func = make_function(
        &db,
        &coll,
        "priv_out_fn",
        Decorator::Publisher,
        &["out", "_priv"],
    )
    .await?;
    let (lfr, pfr) = seed_done_run(&db, &coll, &func).await?;
    stamp_buckets_and_metrics_on_pfr(&db, &pfr, 2, 17, 177).await?;
    bind_all_outputs(&db, &func, &lfr, &pfr).await?;

    let nodes = db
        .metrics()
        .metrics_nodes(&empty_request(Dimension::Collection), &VisibleProjects::All)
        .await?;
    assert_eq!(nodes.len(), 1, "one node expected, got {nodes:?}");
    let n = &nodes[0];
    assert_eq!(
        *n.table_versions_out, 1,
        "private output must not count, got {n:?}"
    );
    assert_eq!(*n.row_count_out, 10);
    assert_eq!(*n.data_bytes_out, 100);

    let edges = db
        .metrics()
        .metrics_edges(&empty_request(Dimension::Collection), &VisibleProjects::All)
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
    assert_eq!(*ingress[0].row_count, 10);
    assert_eq!(unused.len(), 1, "one unused edge, got {edges:?}");
    assert_eq!(unused[0].tables, 1);
    assert_eq!(*unused[0].row_count, 10);
    Ok(())
}

/// A private version consumed across collections surfaces nowhere: no IN
/// magnitudes, no real edge, no ingress for the producer, no unused edge.
#[td_test::test(db)]
async fn test_private_input_excluded_from_in_and_real_edges(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let prod_coll = seed_collection(&db, p.id, "priv_in_prod").await?;
    let prod_fn = make_function(
        &db,
        &prod_coll,
        "priv_in_prod_fn",
        Decorator::Publisher,
        &["_priv"],
    )
    .await?;
    let (prod_lfr, prod_pfr) = seed_done_run(&db, &prod_coll, &prod_fn).await?;
    stamp_buckets_on_pfr(&db, &prod_pfr).await?;
    bind_all_outputs(&db, &prod_fn, &prod_lfr, &prod_pfr).await?;
    let prod_ltdvs: Vec<LogicalTableDataVersionDB> =
        db.timeline().fetch_all(&prod_lfr.base.execution_id).await?;
    let private_ltdv = &prod_ltdvs[0];

    let cons_coll = seed_collection(&db, p.id, "priv_in_cons").await?;
    let cons_fn = make_publisher_function(&db, &cons_coll, "priv_in_cons_fn").await?;
    let (cons_lfr, cons_pfr) = seed_done_run(&db, &cons_coll, &cons_fn).await?;
    stamp_buckets_on_pfr(&db, &cons_pfr).await?;
    link_input(&db, &cons_coll, &cons_lfr, private_ltdv).await?;

    let nodes = db
        .metrics()
        .metrics_nodes(&empty_request(Dimension::Collection), &VisibleProjects::All)
        .await?;
    let prod_node = nodes
        .iter()
        .find(|n| n.collection_id.as_ref() == Some(&prod_coll.id))
        .expect("producer node");
    assert_eq!(
        *prod_node.table_versions_out, 0,
        "private-only producer must report zero OUT, got {prod_node:?}"
    );
    assert_eq!(*prod_node.row_count_out, 0);
    let cons_node = nodes
        .iter()
        .find(|n| n.collection_id.as_ref() == Some(&cons_coll.id))
        .expect("consumer node");
    assert_eq!(
        *cons_node.table_versions_in, 0,
        "private input must not count, got {cons_node:?}"
    );
    assert_eq!(*cons_node.row_count_in, 0);

    let edges = db
        .metrics()
        .metrics_edges(&empty_request(Dimension::Collection), &VisibleProjects::All)
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

/// A consumer whose only outputs are private has no visible outputs, so it
/// classifies as a topological subscriber.
#[td_test::test(db)]
async fn test_private_only_outputs_make_topological_subscriber(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_prod_coll, _, prod_ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "priv_egr_prod", "priv_egr_prod_fn").await?;

    let cons_coll = seed_collection(&db, p.id, "priv_egr_cons").await?;
    let cons_fn = make_function(
        &db,
        &cons_coll,
        "priv_egr_cons_fn",
        Decorator::Publisher,
        &["_state"],
    )
    .await?;
    let (cons_lfr, cons_pfr) = seed_done_run(&db, &cons_coll, &cons_fn).await?;
    stamp_buckets_on_pfr(&db, &cons_pfr).await?;
    link_input(&db, &cons_coll, &cons_lfr, &prod_ltdv).await?;

    let edges = db
        .metrics()
        .metrics_edges(&empty_request(Dimension::Collection), &VisibleProjects::All)
        .await?;
    let egress: Vec<_> = edges
        .iter()
        .filter(|e| e.target_kind == NodeKind::ExternalDestination)
        .collect();
    assert_eq!(egress.len(), 1, "one egress edge, got {edges:?}");
    assert_eq!(egress[0].source, Some(cons_coll.id.to_string()));
    assert_eq!(*egress[0].row_count, 10);
    assert!(
        edges.iter().all(
            |e| e.target_kind != NodeKind::Unused || e.source != Some(cons_coll.id.to_string())
        ),
        "private output must not appear as unused, got {edges:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_soft_deleted_collection_excluded_from_nodes_and_edges(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (coll, _lfr, _ltdv) = seed_publisher_with_ltdv(&db, p.id, "del_coll", "del_fn").await?;
    let req = empty_request(Dimension::Collection);
    assert_eq!(
        db.metrics()
            .metrics_nodes(&req, &VisibleProjects::All)
            .await?
            .len(),
        1
    );
    assert!(
        !db.metrics()
            .metrics_edges(&req, &VisibleProjects::All)
            .await?
            .is_empty()
    );

    soft_delete_collection(&db, &coll).await?;
    assert!(
        db.metrics()
            .metrics_nodes(&req, &VisibleProjects::All)
            .await?
            .is_empty(),
        "a soft-deleted collection must not appear as a node"
    );
    assert!(
        db.metrics()
            .metrics_edges(&req, &VisibleProjects::All)
            .await?
            .is_empty(),
        "a soft-deleted collection's edges must vanish"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_soft_deleted_project_excluded_from_nodes(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_coll, _lfr, _ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "del_proj_coll", "del_proj_fn").await?;
    let req = empty_request(Dimension::Collection);
    assert_eq!(
        db.metrics()
            .metrics_nodes(&req, &VisibleProjects::All)
            .await?
            .len(),
        1
    );

    soft_delete_project(&db, &p).await?;
    assert!(
        db.metrics()
            .metrics_nodes(&req, &VisibleProjects::All)
            .await?
            .is_empty(),
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
    link_input(&db, &consumer_coll, &consumer_lfr, &producer_ltdv).await?;

    let req = empty_request(Dimension::Collection);
    let producer_id = producer_coll.id.to_string();
    assert!(
        db.metrics()
            .metrics_edges(&req, &VisibleProjects::All)
            .await?
            .iter()
            .any(|e| e.source.as_deref() == Some(producer_id.as_str())),
        "the live producer must be a real-edge source before deletion"
    );

    soft_delete_collection(&db, &producer_coll).await?;
    let edges = db
        .metrics()
        .metrics_edges(&req, &VisibleProjects::All)
        .await?;
    assert!(
        edges
            .iter()
            .all(|e| e.source.as_deref() != Some(producer_id.as_str())
                && e.target.as_deref() != Some(producer_id.as_str())),
        "no edge may reference the soft-deleted producer, got {edges:?}"
    );
    let cons_node = db
        .metrics()
        .metrics_nodes(&req, &VisibleProjects::All)
        .await?
        .into_iter()
        .any(|n| n.collection_id.as_ref() == Some(&consumer_coll.id));
    assert!(cons_node, "the live consumer must still be a node");
    Ok(())
}
