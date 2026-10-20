//
// Copyright 2026 Tabsdata Inc.
//

//! A plan resolves each input edge for the function reading it: new
//! versions are those since that function's own previous run.

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
use td_execution::graph::TdGraph;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::execution::{ExecutionDB, ExecutionRequest};
use td_objects::dxo::function::{FunctionDBWithNames, FunctionParam};
use td_objects::dxo::graph::{GraphDependency, GraphEdge, GraphNode};
use td_objects::dxo::logical::LogicalFunctionRunInsertDB;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    AccessTokenId, AtTime, CollectionIdName, CollectionName, DependencyPos, ExecutionId,
    FunctionIdName, LogicalFunctionRunId, LogicalFunctionRunStatus, OverrideDisabledTrigger,
    SelfDependency, SkipIfRunning, Trigger, UserId,
};
use td_objects::types::table_ref::Versions;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

#[td_test::test(db)]
async fn test_new_versions_resolve_per_reading_function(db: TdPool) -> Result<(), TdError> {
    let p = db.seed().test_project().await?;
    let collection = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    db.scoped(p.id)
        .seed()
        .function(&collection, &register("producer", None))
        .await?;
    let first = db
        .scoped(p.id)
        .seed()
        .function(&collection, &register("first", None))
        .await?;
    let second = db
        .scoped(p.id)
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
            .in_project(p.id);
        let response = Services::with_defaults(db.clone())
            .execute_function(request)
            .await?;
        Ok::<ExecutionId, TdError>(response.data.id)
    };
    let run_1 = run().await?;
    let run_2 = run().await?;

    // `first` read the producer's table with run 1; `second` never ran.
    let run_1_transaction: TransactionDB = db
        .scoped(p.id)
        .fetch_all::<TransactionDB>(&run_1)
        .await?
        .remove(0);
    let run_1_execution: ExecutionDB = db.scoped(p.id).fetch_one(&run_1).await?;
    db.scoped(p.id)
        .insert::<LogicalFunctionRunInsertDB>(&LogicalFunctionRunInsertDB {
            id: LogicalFunctionRunId::new(),
            collection_id: collection.id,
            execution_id: run_1.clone(),
            transaction_id: run_1_transaction.base.id.clone(),
            function_id: first.base.function_id,
            function_version_id: first.base.id,
            trigger: Trigger::Dependency,
            triggered_on: run_1_execution.base.triggered_on,
            triggered_by_id: UserId::admin(),
            status: LogicalFunctionRunStatus::Committed,
            group_id: collection.group_id,
        })
        .await?;

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = pools.read().await?.enter_project(&p.id).await?;
    let conn = trx.get(Grant::asserted("plan versions test"));
    let table: TableDBWithNames = conn
        .scd2()
        .fetch_all::<TableDBWithNames>(())
        .await?
        .into_iter()
        .find(|t| t.base.base.name.to_string() == "producer_t0")
        .expect("producer table");
    let first: FunctionDBWithNames = conn.scd2().fetch_one(&first.base.function_id).await?;
    let second: FunctionDBWithNames = conn.scd2().fetch_one(&second.base.function_id).await?;

    let mut graph = Graph::new();
    let (first_node, table_node) = GraphNode::pair(&first, &table)?;
    let (second_node, _) = GraphNode::pair(&second, &table)?;
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
    let plan = ExecutionPlan::new(TdGraph { inner: graph }, &trigger)?;

    let execution: ExecutionDB = conn.fetch_one(&run_2).await?;
    let resolved = resolve_execution_plan_versions(conn, &execution.base, &plan).await?;

    let mut compared = Vec::new();
    for edge in resolved.inner.edge_indices() {
        let (_, target) = resolved.inner.edge_endpoints(edge).unwrap();
        let GraphNode::Function(function) = &resolved.inner[target] else {
            unreachable!()
        };
        let own: Vec<_> = VersionResolver::builder()
            .function_id(&function.function_id)
            .table_id(&table.base.base.table_id)
            .versions(&Versions::New)
            .triggered_on(&AtTime::from(execution.base.triggered_on))
            .build()?
            .resolve(&mut *conn)
            .await?
            .into_iter()
            .map(|v| v.map(|v| v.base.base.id))
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
