//
// Copyright 2026 Tabsdata Inc.
//

//! `/metrics` end to end, from the worker callback to the response.

use crate::Services;
use crate::execution::services::callback::tests::test_callback;
use crate::execution::update_status::tests::{TestExecution, TestFunction, TestTransaction};
use td_common::execution_status::WorkerCallbackStatus;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::metrics::{
    Dimension, EdgeRow, MetricsRequest, MetricsResponse, NodeKind, NodeRow, NodeStatusMeasures,
};
use td_objects::dxo::query_filters::{DimensionFilter, TimeWindow};
use td_objects::dxo::yaml::WorkerOutput;
use td_objects::dxo::yaml::function::{FunctionOutput, TableInfo, WrittenTable};
use td_objects::types::basic::{
    AccessTokenId, CollectionName, ColumnCount, EpochMs, ExecutionStatus, FunctionName,
    IncludePrivate, IncludeReplaced, LogicalFunctionRunStatus, ProjectId, RowCount, SchemaHash,
    TableName, TableNameDto, TransactionStatus, UserId,
};
use td_queries::Queries;
use td_queries::metrics_facts::runs_reattempted;
use td_queries::test_utils::SeedEntry;

/// The test harness reports the worker clock in the first day of the epoch.
const WINDOW_END_MS: i64 = 86_400_000;

fn request(
    statuses: Vec<LogicalFunctionRunStatus>,
    include_replaced: IncludeReplaced,
) -> MetricsRequest {
    MetricsRequest {
        dim: Dimension::Collection,
        window: TimeWindow::new(
            EpochMs::try_from(0).unwrap(),
            EpochMs::try_from(WINDOW_END_MS).unwrap(),
        ),
        collections: DimensionFilter::default(),
        groups: DimensionFilter::default(),
        statuses,
        include_replaced,
        include_private: IncludePrivate::FALSE,
    }
}

fn status_of(node: &NodeRow, status: LogicalFunctionRunStatus) -> &NodeStatusMeasures {
    node.by_status
        .get(&status)
        .unwrap_or_else(|| panic!("no {status:?} measures on {node:?}"))
}

async fn metrics(
    db: &TdPool,
    project: ProjectId,
    req: MetricsRequest,
) -> Result<MetricsResponse, TdError> {
    Ok(Services::with_defaults(db.clone())
        .metrics(
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .read(req)
                .in_project(project),
        )
        .await?
        .data)
}

fn dim_nodes(response: &MetricsResponse) -> Vec<&NodeRow> {
    response
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Dim)
        .collect()
}

fn ingress_edges(response: &MetricsResponse) -> Vec<&EdgeRow> {
    response
        .edges
        .iter()
        .filter(|e| e.source_kind == NodeKind::ExternalSource)
        .collect()
}

async fn visible_output_versions(db: &TdPool) -> Result<i64, TdError> {
    let versions: Vec<LogicalTableDataVersionDB> = db.timeline().fetch_all(()).await?;
    Ok(versions
        .iter()
        .filter(|v| {
            v.base.producer_logical_function_run_id.is_some()
                && v.base.function_param_pos.is_some_and(|p| *p >= 0)
                && !*v.base.private
        })
        .count() as i64)
}

async fn publisher_callback(db: &TdPool, project: ProjectId) -> Result<(), TdError> {
    test_callback(
        db.clone(),
        project,
        vec![TestExecution {
            expected_status: ExecutionStatus::Finished,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Committed,
                functions: vec![TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![
                        TableNameDto::try_from("t_0")?,
                        TableNameDto::try_from("t_1")?,
                    ],
                    initial_status: LogicalFunctionRunStatus::Running,
                    expected_status: LogicalFunctionRunStatus::Committed,
                }],
            }],
        }],
        "f_0",
        WorkerCallbackStatus::Done,
        Some(WorkerOutput::Function(FunctionOutput {
            output: vec![
                WrittenTable::NoData {
                    table: TableName::try_from("t_0")?,
                },
                WrittenTable::Data {
                    table: TableName::try_from("t_1")?,
                    info: TableInfo {
                        column_count: ColumnCount::try_from(1i64)?,
                        row_count: RowCount::try_from(2i64)?,
                        schema_hash: SchemaHash::try_from("hash")?,
                        data_bytes: None,
                    },
                },
            ],
        })),
    )
    .await
}

#[td_test::test(db)]
async fn callback_publishes_the_attempt(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    publisher_callback(&db, p.id).await?;

    let written = visible_output_versions(&db).await?;
    let response = metrics(
        &db,
        p.id,
        request(
            vec![LogicalFunctionRunStatus::Committed],
            IncludeReplaced::FALSE,
        ),
    )
    .await?;
    let nodes = dim_nodes(&response);
    assert_eq!(nodes.len(), 1, "one collection ran: {response:?}");
    let committed = status_of(nodes[0], LogicalFunctionRunStatus::Committed);
    assert_eq!(*committed.fn_runs, 1);
    assert_eq!(
        *nodes[0].logical_tables_out, written,
        "every table the attempt wrote is counted once"
    );
    assert_eq!(*committed.table_versions_out, 1, "only one carries data");
    assert_eq!(*committed.row_count_out, 2);

    let ingress = ingress_edges(&response);
    assert_eq!(ingress.len(), 1, "a publisher is where data enters");
    assert_eq!(
        ingress[0]
            .by_status
            .get(&LogicalFunctionRunStatus::Committed)
            .map(|m| *m.fn_runs),
        Some(1)
    );
    assert_eq!(*ingress[0].logical_tables, written);
    assert!(
        response
            .nodes
            .iter()
            .any(|n| n.kind == NodeKind::ExternalSource),
        "the external end is a node too: {response:?}"
    );
    Ok(())
}

#[td_test::test(db)]
async fn the_run_status_filters_the_attempt(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    publisher_callback(&db, p.id).await?;

    let done = metrics(
        &db,
        p.id,
        request(vec![LogicalFunctionRunStatus::Done], IncludeReplaced::FALSE),
    )
    .await?;
    assert!(done.nodes.is_empty(), "the run committed: {done:?}");

    let committed = metrics(
        &db,
        p.id,
        request(
            vec![LogicalFunctionRunStatus::Committed],
            IncludeReplaced::FALSE,
        ),
    )
    .await?;
    assert_eq!(dim_nodes(&committed).len(), 1);
    Ok(())
}

#[td_test::test(db)]
async fn a_failed_attempt_counts_as_failed(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    test_callback(
        db.clone(),
        p.id,
        vec![TestExecution {
            expected_status: ExecutionStatus::Stalled,
            transactions: vec![TestTransaction {
                expected_status: TransactionStatus::Stalled,
                functions: vec![TestFunction {
                    collection: CollectionName::try_from("c_0")?,
                    name: FunctionName::try_from("f_0")?,
                    dependencies: vec![],
                    tables: vec![TableNameDto::try_from("t_0")?],
                    initial_status: LogicalFunctionRunStatus::Running,
                    expected_status: LogicalFunctionRunStatus::Failed,
                }],
            }],
        }],
        "f_0",
        WorkerCallbackStatus::Failed,
        None,
    )
    .await?;

    let response = metrics(
        &db,
        p.id,
        request(
            vec![LogicalFunctionRunStatus::Failed],
            IncludeReplaced::FALSE,
        ),
    )
    .await?;
    let nodes = dim_nodes(&response);
    assert_eq!(nodes.len(), 1, "the failed attempt is a fact: {response:?}");
    assert_eq!(
        *status_of(nodes[0], LogicalFunctionRunStatus::Failed).fn_runs,
        1
    );
    Ok(())
}

#[td_test::test(db)]
async fn a_replaced_attempt_is_filtered_not_deducted(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    publisher_callback(&db, p.id).await?;
    let lfrs: Vec<LogicalFunctionRunDB> = db.fetch_all(()).await?;
    let lfr = lfrs.first().expect("the callback ran one function");

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = DbPool::in_project(&pools, &p.id).begin_exclusive().await?;
    runs_reattempted(
        trx.get(Grant::asserted("metrics behavior test")),
        std::slice::from_ref(&lfr.base.id),
    )
    .await?;
    trx.commit().await?;

    let default = metrics(
        &db,
        p.id,
        request(
            vec![LogicalFunctionRunStatus::Committed],
            IncludeReplaced::FALSE,
        ),
    )
    .await?;
    assert!(default.nodes.is_empty(), "superseded: {default:?}");

    let replayed = metrics(
        &db,
        p.id,
        request(
            vec![LogicalFunctionRunStatus::Committed],
            IncludeReplaced::TRUE,
        ),
    )
    .await?;
    assert_eq!(dim_nodes(&replayed).len(), 1, "still there: {replayed:?}");
    assert_eq!(
        *status_of(dim_nodes(&replayed)[0], LogicalFunctionRunStatus::Committed).fn_runs,
        1
    );
    Ok(())
}
