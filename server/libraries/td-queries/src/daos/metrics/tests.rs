//
// Copyright 2026 Tabsdata Inc.
//

use crate::Queries;
use crate::daos::metrics::write::{attempts_ended, runs_reattempted, runs_status_changed};
use crate::test_utils::{SeedEntry, begin_exclusive, derive_metrics_facts};
use std::collections::HashMap;
use ta_objects::and;
use td_database::access::LockInstant;
use td_database::access::{Grant, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDeleteDB};
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputInsertDB, LogicalFunctionRunStatusUpdateDB,
    LogicalTableDataVersionDB,
};
use td_objects::dxo::metrics::{Dimension, InflightRequest, MetricsRequest};
use td_objects::dxo::metrics_facts::{MetricsFactDB, MetricsTableRow};
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, PhysicalFunctionRunInsertDB, PhysicalFunctionRunUpdateDB,
};
use td_objects::dxo::physical_table_data_version::{
    PhysicalTableDataVersionDB, PhysicalTableDataVersionInsertDB,
};
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow};
use td_objects::dxo::table::TableDB;
use td_objects::types::basic::{
    AtTime, BasePath, BundleId, ByteCount, CollectionId, CollectionName, Decorator, DependencyPos,
    Description, EdgeKind, EpochMs, FactSide, GroupName, HasData, IncludePrivate, IncludeReplaced,
    InputIdx, LogicalFunctionRunInputId, LogicalFunctionRunStatus, PhysicalFunctionRunId,
    PhysicalFunctionRunStatus, PhysicalTableDataVersionId, ProjectId, ReuseFrozen, RowCount,
    TableFunctionParamPos, TableId, TableNameDto, TransactionKey, UserId, VersionPos,
};
use td_objects::types::visible_projects::VisibleProjects;
use td_security::ENCODED_ID_SYSTEM;

const DAY_MS: i64 = 86_400_000;

/// Attempts end now, so their reads see the materializations seeded before
/// them, and the window is the day around now.
fn in_window() -> Result<AtTime, TdError> {
    Ok(AtTime::now())
}

fn window() -> TimeWindow {
    let now = (**AtTime::now()).timestamp_millis();
    TimeWindow::new(
        EpochMs::try_from(now - DAY_MS).unwrap(),
        EpochMs::try_from(now + DAY_MS).unwrap(),
    )
}

fn system_user() -> Result<UserId, TdError> {
    UserId::try_from(ENCODED_ID_SYSTEM)
}

fn metrics_request(dim: Dimension) -> MetricsRequest {
    MetricsRequest {
        dim,
        window: window(),
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses: LogicalFunctionRunStatus::ENDED.to_vec(),
        include_replaced: IncludeReplaced::FALSE,
        include_private: IncludePrivate::FALSE,
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

async fn end_attempt(db: &TdPool, pfr: &PhysicalFunctionRunDB) -> Result<(), TdError> {
    stamp_status_at(db, pfr, PhysicalFunctionRunStatus::Done).await
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
    (&scoped)
        .scd2()
        .open(&ptdv, LockInstant::asserted(*pfr.interval.valid_from))
        .await?;
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
    end_attempt(db, &pfr).await?;
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

async fn stamp_status_at(
    db: &TdPool,
    pfr: &PhysicalFunctionRunDB,
    status: PhysicalFunctionRunStatus,
) -> Result<(), TdError> {
    let update = PhysicalFunctionRunUpdateDB {
        dispatched_on: None,
        started_on: None,
        ended_on: Some(in_window()?.into()),
        status,
    };
    db.scoped(pfr.project_id)
        .scd2()
        .update(&update, &pfr.base.id)
        .await?;
    Ok(())
}

async fn in_project<F>(db: &TdPool, project: ProjectId, body: F) -> Result<(), TdError>
where
    F: AsyncFnOnce(&mut DomainConnection<Apiserver, InProject>) -> Result<(), TdError>,
{
    let mut trx = begin_exclusive(db, project).await?;
    body(trx.get(Grant::asserted("metrics dao test"))).await?;
    trx.commit().await?;
    Ok(())
}

async fn facts(db: &TdPool) -> Result<Vec<String>, TdError> {
    let rows: Vec<MetricsFactDB> = db.fetch_all(()).await?;
    let mut out: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
    out.sort();
    Ok(out)
}

async fn assert_live_matches_backfill(db: &TdPool) -> Result<(), TdError> {
    let live = facts(db).await?;
    assert!(!live.is_empty(), "the seeded runs produced no facts");
    derive_metrics_facts(db).await?;
    assert_eq!(live, facts(db).await?, "the writers and the rung disagree");
    Ok(())
}

async fn aggregates(db: &TdPool, dim: Dimension) -> Result<Vec<MetricsTableRow>, TdError> {
    Ok(db
        .metrics()
        .per_table(&metrics_request(dim), &VisibleProjects::All)
        .await?)
}

fn edges_of(rows: &[MetricsTableRow], kind: EdgeKind) -> Vec<&MetricsTableRow> {
    rows.iter().filter(|r| r.edge_kind == Some(kind)).collect()
}

fn unconsumed_of(rows: &[MetricsTableRow], collection: &CollectionId) -> i64 {
    rows.iter()
        .filter(|r| r.side == FactSide::Written && r.collection_id == *collection)
        .map(|r| r.versions_unconsumed)
        .sum()
}

fn tables_unread_of(rows: &[MetricsTableRow], collection: &CollectionId) -> i64 {
    let mut per_table: HashMap<Option<TableId>, (i64, i64)> = HashMap::new();
    for r in rows
        .iter()
        .filter(|r| r.side == FactSide::Written && r.collection_id == *collection)
    {
        let (versions, unconsumed) = per_table.entry(r.table_id).or_default();
        *versions += r.versions;
        *unconsumed += r.versions_unconsumed;
    }
    per_table
        .values()
        .filter(|(versions, unconsumed)| *unconsumed > 0 && unconsumed == versions)
        .count() as i64
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
async fn test_inflight_keeps_soft_deleted_collection(db: TdPool) -> Result<(), TdError> {
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
    let rows = db.metrics().inflight(&req, &VisibleProjects::All).await?;
    assert_eq!(rows.len(), 1, "the run is still in flight, got {rows:?}");
    assert_eq!(rows[0].collection_name, coll.name);
    Ok(())
}

#[td_test::test(db)]
async fn test_empty_window_has_no_facts(db: TdPool) -> Result<(), TdError> {
    db.seed().test_project().await?;
    assert!(aggregates(&db, Dimension::Collection).await?.is_empty());
    Ok(())
}

#[td_test::test(db)]
async fn test_publisher_writes_a_source_edge(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (coll, lfr, _) = seed_publisher_with_ltdv(&db, p.id, "pub_coll", "pub_fn").await?;
    let pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&pfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    let attempts: Vec<_> = rows
        .iter()
        .filter(|r| r.side == FactSide::Attempt)
        .collect();
    assert_eq!(attempts.len(), 1, "one attempt row, got {rows:?}");
    assert_eq!(attempts[0].fn_runs, 1);
    assert_eq!(attempts[0].collection_id, coll.id);

    let source = edges_of(&rows, EdgeKind::ExternalSource);
    assert_eq!(source.len(), 1, "one ingress edge, got {rows:?}");
    assert_eq!(source[0].fn_runs, 1);
    assert_eq!(source[0].versions, 1);
    assert_eq!(source[0].row_count, 10);
    assert_eq!(source[0].data_bytes, 100);
    assert!(
        edges_of(&rows, EdgeKind::Egress).is_empty(),
        "a publisher reads nothing, so nothing leaves through it"
    );
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_transformer_reads_a_real_edge(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (source_coll, source_lfr, ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "src_coll", "src_fn").await?;
    let target_coll = seed_collection(&db, p.id, "tgt_coll").await?;
    let target_fn = make_function(
        &db,
        &target_coll,
        "tgt_fn",
        Decorator::Transformer,
        &["out"],
    )
    .await?;
    let (target_lfr, target_pfr) = seed_done_run(&db, &target_coll, &target_fn).await?;
    end_attempt(&db, &target_pfr).await?;
    link_input(&db, &target_coll, &target_lfr, &ltdv).await?;
    bind_all_outputs(&db, &target_fn, &target_lfr, &target_pfr).await?;

    let source_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&source_lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(
            conn,
            &[source_pfr.base.id.clone(), target_pfr.base.id.clone()],
        )
        .await
        .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    let real = edges_of(&rows, EdgeKind::Real);
    assert_eq!(real.len(), 1, "one real edge, got {rows:?}");
    assert_eq!(real[0].from_collection_id, Some(source_coll.id));
    assert_eq!(real[0].collection_id, target_coll.id);
    assert_eq!(real[0].fn_runs, 1);
    assert!(
        edges_of(&rows, EdgeKind::Egress).is_empty(),
        "the transformer writes a visible table, so nothing leaves the system"
    );
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_written_version_is_unconsumed_until_a_run_binds_it(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (source_coll, source_lfr, ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "unread_coll", "unread_fn").await?;
    let source_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&source_lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&source_pfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    assert_eq!(
        unconsumed_of(&rows, &source_coll.id),
        1,
        "nothing has read the published version, got {rows:?}"
    );
    assert_eq!(
        tables_unread_of(&rows, &source_coll.id),
        1,
        "every version of the published table is unread, got {rows:?}"
    );

    let target_coll = seed_collection(&db, p.id, "reader_coll").await?;
    let target_fn = make_function(
        &db,
        &target_coll,
        "reader_fn",
        Decorator::Transformer,
        &["out"],
    )
    .await?;
    let (target_lfr, target_pfr) = seed_done_run(&db, &target_coll, &target_fn).await?;
    end_attempt(&db, &target_pfr).await?;
    link_input(&db, &target_coll, &target_lfr, &ltdv).await?;
    bind_all_outputs(&db, &target_fn, &target_lfr, &target_pfr).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&target_pfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    assert_eq!(
        unconsumed_of(&rows, &source_coll.id),
        0,
        "the transformer bound the version, got {rows:?}"
    );
    assert_eq!(
        tables_unread_of(&rows, &source_coll.id),
        0,
        "the published table is read, got {rows:?}"
    );
    assert_eq!(
        unconsumed_of(&rows, &target_coll.id),
        1,
        "nothing reads what the transformer wrote, got {rows:?}"
    );
    assert_eq!(
        tables_unread_of(&rows, &target_coll.id),
        1,
        "the transformer's own table is never read, got {rows:?}"
    );
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_subscriber_yields_an_egress_edge(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, source_lfr, ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "sub_src", "sub_src_fn").await?;
    let coll = seed_collection(&db, p.id, "sub_coll").await?;
    let func = make_function(&db, &coll, "sub_fn", Decorator::Subscriber, &[]).await?;
    let (lfr, pfr) = seed_done_run(&db, &coll, &func).await?;
    end_attempt(&db, &pfr).await?;
    link_input(&db, &coll, &lfr, &ltdv).await?;

    let source_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&source_lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, &[source_pfr.base.id.clone(), pfr.base.id.clone()])
            .await
            .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    let egress = edges_of(&rows, EdgeKind::Egress);
    assert_eq!(egress.len(), 1, "one egress edge, got {rows:?}");
    assert_eq!(egress[0].collection_id, coll.id);
    assert_eq!(egress[0].fn_runs, 1, "the attempt is counted once");
    assert_eq!(egress[0].row_count, 10);
    assert_live_matches_backfill(&db).await
}

/// A private-only writer keeps its data inside the system: not an egress.
#[td_test::test(db)]
async fn test_private_outputs_are_marked_and_shown_on_request(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, source_lfr, ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "priv_src", "priv_src_fn").await?;
    let coll = seed_collection(&db, p.id, "priv_coll").await?;
    let func = make_function(&db, &coll, "priv_fn", Decorator::Transformer, &["_priv"]).await?;
    let (lfr, pfr) = seed_done_run(&db, &coll, &func).await?;
    end_attempt(&db, &pfr).await?;
    link_input(&db, &coll, &lfr, &ltdv).await?;
    bind_all_outputs(&db, &func, &lfr, &pfr).await?;

    let source_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&source_lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, &[source_pfr.base.id.clone(), pfr.base.id.clone()])
            .await
            .map_err(Into::into)
    })
    .await?;

    let hidden = aggregates(&db, Dimension::Collection).await?;
    assert!(
        hidden
            .iter()
            .all(|r| r.side != FactSide::Written || r.collection_id != coll.id),
        "the private version is hidden by default, got {hidden:?}"
    );
    assert!(
        edges_of(&hidden, EdgeKind::Egress).is_empty(),
        "a private-only writer keeps its data inside the system, got {hidden:?}"
    );

    let mut req = metrics_request(Dimension::Collection);
    req.include_private = IncludePrivate::TRUE;
    let rows = db.metrics().per_table(&req, &VisibleProjects::All).await?;
    let written: Vec<_> = rows
        .iter()
        .filter(|r| r.side == FactSide::Written && r.collection_id == coll.id)
        .collect();
    assert_eq!(written.len(), 1, "shown on request, got {rows:?}");
    assert_eq!(written[0].row_count, 7);
    assert!(edges_of(&rows, EdgeKind::Egress).is_empty());
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_an_attempt_ended_twice_owns_its_facts_once(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, lfr, _) = seed_publisher_with_ltdv(&db, p.id, "twice_coll", "twice_fn").await?;
    let pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&lfr.base.id).await?;
    for _ in 0..2 {
        in_project(&db, p.id, async |conn| {
            attempts_ended(conn, std::slice::from_ref(&pfr.base.id))
                .await
                .map_err(Into::into)
        })
        .await?;
    }
    let rows = aggregates(&db, Dimension::Collection).await?;
    let attempts: Vec<_> = rows
        .iter()
        .filter(|r| r.side == FactSide::Attempt)
        .collect();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].fn_runs, 1, "got {rows:?}");
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_group_dim_counts_a_run_once_per_source_group(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let group = db
        .scoped(p.id)
        .seed()
        .group(
            GroupName::try_from("shared")?,
            Description::try_from("shared group")?,
        )
        .await?;
    let mut ltdvs = Vec::new();
    let mut ended = Vec::new();
    for name in ["grp_a", "grp_b"] {
        let coll = db
            .scoped(p.id)
            .seed()
            .collection_in_group(&CollectionName::try_from(name)?, &system_user()?, &group)
            .await?;
        let func = make_publisher_function(&db, &coll, &format!("{name}_fn")).await?;
        let (lfr, pfr) = seed_done_run(&db, &coll, &func).await?;
        end_attempt(&db, &pfr).await?;
        bind_all_outputs(&db, &func, &lfr, &pfr).await?;
        let versions: Vec<LogicalTableDataVersionDB> =
            db.timeline().fetch_all(&lfr.base.execution_id).await?;
        ltdvs.extend(
            versions
                .into_iter()
                .filter(|t| t.base.producer_logical_function_run_id.as_ref() == Some(&lfr.base.id)),
        );
        ended.push(pfr.base.id.clone());
    }
    let reader_coll = seed_collection(&db, p.id, "grp_reader").await?;
    let reader_fn = make_function(
        &db,
        &reader_coll,
        "grp_reader_fn",
        Decorator::Transformer,
        &["out"],
    )
    .await?;
    let (reader_lfr, reader_pfr) = seed_done_run(&db, &reader_coll, &reader_fn).await?;
    end_attempt(&db, &reader_pfr).await?;
    for ltdv in &ltdvs {
        link_input(&db, &reader_coll, &reader_lfr, ltdv).await?;
    }
    bind_all_outputs(&db, &reader_fn, &reader_lfr, &reader_pfr).await?;
    ended.push(reader_pfr.base.id.clone());
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, &ended).await.map_err(Into::into)
    })
    .await?;

    let by_collection = aggregates(&db, Dimension::Collection).await?;
    let runs: i64 = edges_of(&by_collection, EdgeKind::Real)
        .iter()
        .map(|r| r.fn_runs)
        .sum();
    assert_eq!(
        runs, 2,
        "one run per source collection, got {by_collection:?}"
    );

    let by_group = aggregates(&db, Dimension::Group).await?;
    let real = edges_of(&by_group, EdgeKind::Real);
    assert!(
        real.iter().all(|r| r.from_group_id == Some(group.base.id)),
        "got {by_group:?}"
    );
    let runs: i64 = real.iter().map(|r| r.fn_runs).sum();
    assert_eq!(runs, 1, "one run on the group edge, got {by_group:?}");
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_collection_filter_keeps_either_endpoint(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (source_coll, source_lfr, ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "flt_src", "flt_src_fn").await?;
    let target_coll = seed_collection(&db, p.id, "flt_tgt").await?;
    let target_fn = make_function(
        &db,
        &target_coll,
        "flt_tgt_fn",
        Decorator::Transformer,
        &["out"],
    )
    .await?;
    let (target_lfr, target_pfr) = seed_done_run(&db, &target_coll, &target_fn).await?;
    end_attempt(&db, &target_pfr).await?;
    link_input(&db, &target_coll, &target_lfr, &ltdv).await?;
    bind_all_outputs(&db, &target_fn, &target_lfr, &target_pfr).await?;
    let (other_coll, other_lfr, _) =
        seed_publisher_with_ltdv(&db, p.id, "flt_other", "flt_other_fn").await?;

    let source_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&source_lfr.base.id).await?;
    let other_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&other_lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(
            conn,
            &[
                source_pfr.base.id.clone(),
                target_pfr.base.id.clone(),
                other_pfr.base.id.clone(),
            ],
        )
        .await
        .map_err(Into::into)
    })
    .await?;

    let mut req = metrics_request(Dimension::Collection);
    req.collections = DimensionFilter::new(vec![target_coll.id], vec![]);
    let rows = db.metrics().per_table(&req, &VisibleProjects::All).await?;
    assert!(
        rows.iter()
            .all(|r| r.collection_id == target_coll.id
                || r.from_collection_id == Some(target_coll.id)),
        "got {rows:?}"
    );
    assert_eq!(
        edges_of(&rows, EdgeKind::Real).len(),
        1,
        "the edge into the target survives"
    );
    assert!(
        rows.iter().all(|r| r.collection_id != other_coll.id),
        "the unrelated collection is gone"
    );

    let mut req = metrics_request(Dimension::Collection);
    req.collections = DimensionFilter::new(vec![], vec![source_coll.id]);
    let rows = db.metrics().per_table(&req, &VisibleProjects::All).await?;
    assert!(
        rows.iter()
            .all(|r| r.collection_id != source_coll.id
                && r.from_collection_id != Some(source_coll.id)),
        "the excluded collection is gone from both ends, got {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.collection_id == other_coll.id),
        "the unrelated collection stays"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_commit_moves_the_run_status(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, lfr, _) = seed_publisher_with_ltdv(&db, p.id, "commit_coll", "commit_fn").await?;
    let pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&pfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let before = aggregates(&db, Dimension::Collection).await?;
    assert!(
        before
            .iter()
            .all(|r| r.run_status == LogicalFunctionRunStatus::Done),
        "got {before:?}"
    );

    in_project(&db, p.id, async |conn| {
        runs_status_changed(
            conn,
            std::slice::from_ref(&lfr.base.id),
            &LogicalFunctionRunStatus::Committed,
        )
        .await
        .map_err(Into::into)
    })
    .await?;

    let after = aggregates(&db, Dimension::Collection).await?;
    assert!(
        after
            .iter()
            .all(|r| r.run_status == LogicalFunctionRunStatus::Committed),
        "got {after:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_failed_attempt_keeps_its_outcome(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = seed_collection(&db, p.id, "fail_coll").await?;
    let func = make_publisher_function(&db, &coll, "fail_fn").await?;
    let (lfr, pfr) = seed_run(&db, &coll, &func, LogicalFunctionRunStatus::Failed).await?;
    stamp_status_at(&db, &pfr, PhysicalFunctionRunStatus::Failed).await?;

    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&pfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;
    in_project(&db, p.id, async |conn| {
        runs_status_changed(
            conn,
            std::slice::from_ref(&lfr.base.id),
            &LogicalFunctionRunStatus::Committed,
        )
        .await
        .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    assert!(
        rows.iter()
            .all(|r| r.run_status == LogicalFunctionRunStatus::Failed),
        "a failed attempt does not become a committed one: {rows:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_replaced_attempt_is_filtered_not_deducted(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, lfr, _) = seed_publisher_with_ltdv(&db, p.id, "replay_coll", "replay_fn").await?;
    let first: PhysicalFunctionRunDB = db.scd2().fetch_one(&lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&first.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let replay = PhysicalFunctionRunInsertDB {
        id: PhysicalFunctionRunId::new(),
        status: PhysicalFunctionRunStatus::Requested,
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        ..first.base.clone()
    };
    db.scoped(p.id)
        .scd2()
        .open(&replay, LockInstant::asserted(*AtTime::now()))
        .await?;
    in_project(&db, p.id, async |conn| {
        runs_reattempted(conn, std::slice::from_ref(&lfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let visible = aggregates(&db, Dimension::Collection).await?;
    assert!(
        visible.is_empty(),
        "the superseded attempt is filtered out, got {visible:?}"
    );

    let mut req = metrics_request(Dimension::Collection);
    req.include_replaced = IncludeReplaced::TRUE;
    let all = db.metrics().per_table(&req, &VisibleProjects::All).await?;
    assert!(
        all.iter()
            .any(|r| r.side == FactSide::Attempt && r.fn_runs == 1),
        "the rows are still there to be counted, got {all:?}"
    );
    assert_live_matches_backfill(&db).await
}

/// A producer replayed after its output was read: the reader keeps the
/// magnitudes it read, and the re-materialized version inherits its
/// consumption, so nothing shows as unread.
#[td_test::test(db)]
async fn test_replaying_a_consumed_producer_keeps_what_was_read(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (source_coll, source_lfr, ltdv) =
        seed_publisher_with_ltdv(&db, p.id, "rp_src", "rp_src_fn").await?;
    let target_coll = seed_collection(&db, p.id, "rp_tgt").await?;
    let target_fn = make_function(
        &db,
        &target_coll,
        "rp_tgt_fn",
        Decorator::Transformer,
        &["out"],
    )
    .await?;
    let (target_lfr, target_pfr) = seed_done_run(&db, &target_coll, &target_fn).await?;
    end_attempt(&db, &target_pfr).await?;
    link_input(&db, &target_coll, &target_lfr, &ltdv).await?;
    bind_all_outputs(&db, &target_fn, &target_lfr, &target_pfr).await?;
    let first: PhysicalFunctionRunDB = db.scd2().fetch_one(&source_lfr.base.id).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, &[first.base.id.clone(), target_pfr.base.id.clone()])
            .await
            .map_err(Into::into)
    })
    .await?;

    let replay = PhysicalFunctionRunInsertDB {
        id: PhysicalFunctionRunId::new(),
        status: PhysicalFunctionRunStatus::Requested,
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        ..first.base.clone()
    };
    db.scoped(p.id)
        .scd2()
        .open(&replay, LockInstant::asserted(*AtTime::now()))
        .await?;
    in_project(&db, p.id, async |conn| {
        runs_reattempted(conn, std::slice::from_ref(&source_lfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;
    let replay: PhysicalFunctionRunDB = db.timeline().fetch_one(&replay.id).await?;
    let tables: Vec<TableDB> = db
        .scd2()
        .fetch_all::<TableDB>(&source_lfr.base.function_id)
        .await?;
    let table = tables
        .iter()
        .find(|t| t.base.table_id == ltdv.base.table_id)
        .expect("the publisher's table");
    bind_ptdv_with_magnitudes(&db, &ltdv, table, &replay, 20, 200).await?;
    end_attempt(&db, &replay).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&replay.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    let read: Vec<_> = rows
        .iter()
        .filter(|r| r.side == FactSide::Read && r.collection_id == target_coll.id)
        .collect();
    assert_eq!(read.len(), 1, "got {rows:?}");
    assert_eq!(read[0].row_count, 10, "the reader keeps what it read");
    assert_eq!(
        unconsumed_of(&rows, &source_coll.id),
        0,
        "the re-materialized version was read before, got {rows:?}"
    );
    let written: Vec<_> = rows
        .iter()
        .filter(|r| r.side == FactSide::Written && r.collection_id == source_coll.id)
        .collect();
    assert_eq!(
        written[0].row_count, 20,
        "the current attempt's magnitudes, got {rows:?}"
    );
    assert_live_matches_backfill(&db).await
}

/// Recovery supersedes the failed attempt exactly as a replay would; the rung
/// must derive the same rows.
#[td_test::test(db)]
async fn test_a_recovered_run_supersedes_its_failed_attempt(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let coll = seed_collection(&db, p.id, "recover_coll").await?;
    let func = make_publisher_function(&db, &coll, "recover_fn").await?;
    let (lfr, failed) = seed_run(&db, &coll, &func, LogicalFunctionRunStatus::Failed).await?;
    stamp_status_at(&db, &failed, PhysicalFunctionRunStatus::Failed).await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&failed.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;

    let retry = PhysicalFunctionRunInsertDB {
        id: PhysicalFunctionRunId::new(),
        status: PhysicalFunctionRunStatus::Requested,
        dispatched_on: None,
        started_on: None,
        ended_on: None,
        ..failed.base.clone()
    };
    db.scoped(p.id)
        .scd2()
        .open(&retry, LockInstant::asserted(*AtTime::now()))
        .await?;
    in_project(&db, p.id, async |conn| {
        runs_reattempted(conn, std::slice::from_ref(&lfr.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;
    let retry: PhysicalFunctionRunDB = db.timeline().fetch_one(&retry.id).await?;
    stamp_status_at(&db, &retry, PhysicalFunctionRunStatus::Done).await?;
    bind_all_outputs(&db, &func, &lfr, &retry).await?;
    db.scoped(p.id)
        .update(
            &LogicalFunctionRunStatusUpdateDB {
                status: LogicalFunctionRunStatus::Committed,
            },
            &lfr.base.id,
        )
        .await?;
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, std::slice::from_ref(&retry.base.id))
            .await
            .map_err(Into::into)
    })
    .await?;
    in_project(&db, p.id, async |conn| {
        runs_status_changed(
            conn,
            std::slice::from_ref(&lfr.base.id),
            &LogicalFunctionRunStatus::Committed,
        )
        .await
        .map_err(Into::into)
    })
    .await?;

    let current = aggregates(&db, Dimension::Collection).await?;
    assert!(
        current
            .iter()
            .all(|r| r.run_status == LogicalFunctionRunStatus::Committed),
        "only the retry is current, got {current:?}"
    );
    let mut req = metrics_request(Dimension::Collection);
    req.include_replaced = IncludeReplaced::TRUE;
    let all = db.metrics().per_table(&req, &VisibleProjects::All).await?;
    let attempts: i64 = all
        .iter()
        .filter(|r| r.side == FactSide::Attempt)
        .map(|r| r.fn_runs)
        .sum();
    assert_eq!(attempts, 2, "both attempts are facts, got {all:?}");
    assert!(
        all.iter()
            .any(|r| r.run_status == LogicalFunctionRunStatus::Failed),
        "the failed attempt keeps its outcome, got {all:?}"
    );
    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_mixed_project_matches_backfill(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let (_, publisher, ltdv) = seed_publisher_with_ltdv(&db, p.id, "mix_pub", "mix_pub_fn").await?;

    let mid = seed_collection(&db, p.id, "mix_mid").await?;
    let mid_fn = make_function(
        &db,
        &mid,
        "mix_mid_fn",
        Decorator::Transformer,
        &["out", "_priv"],
    )
    .await?;
    let (mid_lfr, mid_pfr) = seed_done_run(&db, &mid, &mid_fn).await?;
    end_attempt(&db, &mid_pfr).await?;
    link_input(&db, &mid, &mid_lfr, &ltdv).await?;
    bind_all_outputs(&db, &mid_fn, &mid_lfr, &mid_pfr).await?;

    let sink = seed_collection(&db, p.id, "mix_sink").await?;
    let sink_fn = make_function(&db, &sink, "mix_sink_fn", Decorator::Subscriber, &[]).await?;
    let (sink_lfr, sink_pfr) = seed_done_run(&db, &sink, &sink_fn).await?;
    end_attempt(&db, &sink_pfr).await?;
    link_input(&db, &sink, &sink_lfr, &ltdv).await?;

    let failed_fn = make_publisher_function(&db, &mid, "mix_failed_fn").await?;
    let (_, failed_pfr) = seed_run(&db, &mid, &failed_fn, LogicalFunctionRunStatus::Failed).await?;
    stamp_status_at(&db, &failed_pfr, PhysicalFunctionRunStatus::Failed).await?;

    let publisher_pfr: PhysicalFunctionRunDB = db.scd2().fetch_one(&publisher.base.id).await?;
    let ended = vec![
        publisher_pfr.base.id.clone(),
        mid_pfr.base.id.clone(),
        sink_pfr.base.id.clone(),
        failed_pfr.base.id.clone(),
    ];
    in_project(&db, p.id, async |conn| {
        attempts_ended(conn, &ended).await.map_err(Into::into)
    })
    .await?;

    assert_live_matches_backfill(&db).await
}

#[td_test::test(db)]
async fn test_the_rung_derives_a_cold_database(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    seed_publisher_with_ltdv(&db, p.id, "cold_coll", "cold_fn").await?;
    assert!(aggregates(&db, Dimension::Collection).await?.is_empty());

    derive_metrics_facts(&db).await?;

    let rows = aggregates(&db, Dimension::Collection).await?;
    assert!(!rows.is_empty(), "the rung derived nothing");
    assert_eq!(edges_of(&rows, EdgeKind::ExternalSource).len(), 1);
    Ok(())
}
