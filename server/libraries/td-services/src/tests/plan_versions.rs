//
// Copyright 2026 Tabsdata Inc.
//

//! A plan resolves each input edge for the function reading it: new
//! versions are those since that function's own previous run that was not
//! cancelled.

use crate::Services;
use crate::execution::plan::versions::resolve_execution_plan_versions;
use crate::tests::fixtures::register;
use petgraph::Graph;
use std::str::FromStr;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_database::pool::scoped::ScopedExt as _;
use td_error::TdError;
use td_execution::ExecutionPlan;
use td_execution::VersionResolver;
use td_execution::graph::TopologyGraph;
use td_execution::topology::ProjectTopology;
use td_objects::and;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionRequest};
use td_objects::dxo::function::{FunctionParam, FunctionRegister};
use td_objects::dxo::graph::{GraphDependency, GraphEdge, GraphNode, TableNode};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, LogicalFunctionRunInputDB, LogicalFunctionRunInsertDB,
    LogicalTableDataVersionDB,
};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::{TransactionDB, TransactionParam};
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionIdName, CollectionName, DependencyPos, ExecutionId,
    FunctionIdName, LogicalFunctionRunId, LogicalFunctionRunStatus, LogicalTableDataVersionId,
    OverrideDisabledTrigger, ProjectId, SelfDependency, SkipIfRunning, TableName, Trigger, UserId,
};
use td_objects::types::composed::TableDependencyDto;
use td_objects::types::table_ref::Versions;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_new_versions_resolve_per_reading_function(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    db.scoped(p.project_id)
        .seed()
        .function(&collection, &register("producer", None))
        .await?;
    let first = db
        .scoped(p.project_id)
        .seed()
        .function(&collection, &register("first", None))
        .await?;
    let second = db
        .scoped(p.project_id)
        .seed()
        .function(&collection, &register("second", None))
        .await?;

    let run = || async {
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str("c")?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("producer")?,
                    },
                ),
                ExecutionRequest {
                    name: None,
                    function_only: true.into(),
                    override_disabled_trigger: OverrideDisabledTrigger::default(),
                    skip_if_running: SkipIfRunning::default(),
                    external_uid: None,
                },
            )
            .in_project(p.project_id);
        let response = Services::with_defaults(db.clone())
            .execute_function(request)
            .await?;
        Ok::<ExecutionId, TdError>(response.data.id)
    };
    let run_1 = run().await?;
    let run_2 = run().await?;

    // `first` read the producer's table with run 1; `second` never ran.
    let run_1_transaction: TransactionDB = db
        .scoped(p.project_id)
        .fetch_all::<TransactionDB>(&run_1)
        .await?
        .remove(0);
    let run_1_execution: ExecutionDB = db.scoped(p.project_id).fetch_one(&run_1).await?;
    db.scoped(p.project_id)
        .insert::<LogicalFunctionRunInsertDB>(&LogicalFunctionRunInsertDB {
            id: LogicalFunctionRunId::new(),
            collection_id: collection.collection_id,
            execution_id: run_1.clone(),
            transaction_id: run_1_transaction.id.clone(),
            function_id: first.function_id,
            function_version_id: first.base.id,
            trigger: Trigger::Dependency,
            triggered_on: run_1_execution.triggered_on,
            triggered_by_id: UserId::admin(),
            status: LogicalFunctionRunStatus::Committed,
            group_id: collection.group_id,
        })
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = pools.read().await?.enter_project(&p.project_id).await?;
    let conn = trx.get(Grant::asserted("plan versions test"));
    let project = ProjectTopology::current(conn, &p.project_id)
        .await?
        .graph()?;
    let node = |found: &dyn Fn(&GraphNode) -> bool| {
        project
            .inner
            .node_weights()
            .find(|n| found(n))
            .cloned()
            .expect("node")
    };
    let first_node =
        node(&|n| matches!(n, GraphNode::Function(f) if f.function_id == first.base.function_id));
    let second_node =
        node(&|n| matches!(n, GraphNode::Function(f) if f.function_id == second.function_id));
    let table_node =
        node(&|n| matches!(n, GraphNode::Table(t) if t.name.to_string() == "producer_t0"));

    let GraphNode::Table(TableNode { table_id, .. }) = table_node else {
        unreachable!()
    };
    let mut graph = Graph::new();
    let first_idx = graph.add_node(first_node.clone());
    let second_idx = graph.add_node(second_node);
    let table_idx = graph.add_node(table_node);
    let reads_new = || {
        GraphEdge::dependency(
            Versions::New,
            GraphDependency {
                dep_pos: DependencyPos::try_from(0).unwrap(),
                self_dependency: SelfDependency::FALSE,
            },
        )
    };
    graph.add_edge(table_idx, first_idx, reads_new());
    graph.add_edge(table_idx, second_idx, reads_new());
    let GraphNode::Function(trigger) = first_node else {
        unreachable!()
    };
    let plan = ExecutionPlan::new(TopologyGraph { inner: graph }, &trigger)?;

    let execution: ExecutionDB = conn.fetch_one(&run_2).await?;
    let resolved = resolve_execution_plan_versions(conn, &execution, &plan).await?;

    let mut compared = Vec::new();
    for edge in resolved.inner.edge_indices() {
        let (_, target) = resolved.inner.edge_endpoints(edge).unwrap();
        let GraphNode::Function(function) = &resolved.inner[target] else {
            unreachable!()
        };
        let own: Vec<_> = VersionResolver::builder()
            .function_id(&function.function_id)
            .table_id(&table_id)
            .versions(&Versions::New)
            .triggered_on(&AtTime::from(execution.triggered_on))
            .build()?
            .resolve(&mut *conn)
            .await?
            .into_iter()
            .map(|v| v.map(|v| v.id.clone()))
            .collect();
        assert_eq!(
            resolved.inner[edge].versions().inner,
            own,
            "{} reads its own new versions",
            function.name
        );
        compared.push(own.len());
    }
    compared.sort();
    assert_eq!(compared, vec![1, 2]);
    Ok(())
}

async fn execute(db: &TdPool, project: ProjectId, function: &str) -> Result<ExecutionId, TdError> {
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create(
            (
                CollectionParam {
                    collection: CollectionIdName::from_str("c")?,
                },
                FunctionParam {
                    function: FunctionIdName::from_str(function)?,
                },
            ),
            ExecutionRequest {
                name: None,
                function_only: true.into(),
                override_disabled_trigger: OverrideDisabledTrigger::default(),
                skip_if_running: SkipIfRunning::default(),
                external_uid: None,
            },
        )
        .in_project(project);
    Ok(Services::with_defaults(db.clone())
        .execute_function(request)
        .await?
        .data
        .id)
}

async fn only_run(
    db: &TdPool,
    project: ProjectId,
    execution: &ExecutionId,
) -> Result<LogicalFunctionRunDB, TdError> {
    let mut runs: Vec<LogicalFunctionRunDB> = db.scoped(project).fetch_all(execution).await?;
    assert_eq!(runs.len(), 1, "a function-only plan runs one function");
    Ok(runs.remove(0))
}

#[td_test::test(db)]
async fn test_new_versions_of_a_cancelled_run_go_to_the_next_plan(
    db: TdPool,
) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.project_id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    db.scoped(p.project_id)
        .seed()
        .function(&collection, &register("producer", None))
        .await?;
    let consumer = FunctionRegister {
        dependencies: Some(vec![TableDependencyDto::try_from("producer_t0@NEW")?]),
        triggers: Some(vec![]),
        ..register("consumer", None)
    };
    db.scoped(p.project_id)
        .seed()
        .function(&collection, &consumer)
        .await?;
    let table: TableDB = db
        .scd2()
        .fetch_first(and!(
            &collection.collection_id,
            &TableName::try_from("producer_t0")?
        ))
        .await?
        .expect("producer_t0 is registered");

    execute(&db, p.project_id, "producer").await?;
    execute(&db, p.project_id, "consumer").await?;
    let second = execute(&db, p.project_id, "producer").await?;
    let cancelled = execute(&db, p.project_id, "consumer").await?;
    let cancelled = only_run(&db, p.project_id, &cancelled).await?;
    let request = RequestContext::with(AccessTokenId::default(), UserId::admin()).update(
        TransactionParam {
            transaction: cancelled.transaction_id.clone(),
        },
        (),
    );
    Services::with_defaults(db.clone())
        .transaction_cancel(request)
        .await?;
    let third = execute(&db, p.project_id, "producer").await?;
    let next = execute(&db, p.project_id, "consumer").await?;
    let next = only_run(&db, p.project_id, &next).await?;

    let inputs: Vec<LogicalFunctionRunInputDB> = db
        .scoped(p.project_id)
        .chunked()
        .fetch_all(std::slice::from_ref(&next.id), ())
        .await?;
    let pinned: Vec<LogicalTableDataVersionId> = inputs
        .iter()
        .filter_map(|i| i.requirement_logical_table_data_version_id.clone())
        .collect();
    let mut read_from: Vec<ExecutionId> = db
        .scoped(p.project_id)
        .timeline()
        .chunked()
        .fetch_all::<LogicalTableDataVersionDB>(&pinned, ())
        .await?
        .into_iter()
        .filter(|v| v.base.table_id == table.table_id)
        .map(|v| v.base.execution_id)
        .collect();
    read_from.sort();
    assert_eq!(
        read_from,
        vec![second, third],
        "the consumer's next run takes the versions its cancelled run held"
    );
    Ok(())
}
