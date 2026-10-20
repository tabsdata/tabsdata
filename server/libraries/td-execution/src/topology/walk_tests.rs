//
// Copyright 2026 Tabsdata Inc.
//

//! The walks over one seeded topology, edge by edge.

use crate::topology::plan::TopologyTriggers;
use crate::topology::walk::Admitted;
use crate::topology::{FunctionState, ProjectTopology};
use std::collections::{BTreeSet, HashMap};
use ta_execution::TriggerSource;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
use td_objects::dxo::graph::GraphNode;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::topology::{TopologyDependency, TopologyTrigger};
use td_objects::types::basic::{
    BundleId, CollectionName, Decorator, Depth, Enabled, FunctionId, FunctionName,
    FunctionRuntimeValues, OverrideDisabledTrigger, ReuseFrozen, TableNameDto, UserId,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_objects::types::recursion::RecursionOptions;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

fn some<T>(v: Vec<T>) -> Option<Vec<T>> {
    (!v.is_empty()).then_some(v)
}

fn register(
    name: &str,
    tables: &[&str],
    triggers: &[&str],
    dependencies: &[&str],
    enabled: bool,
) -> Result<FunctionRegister, TdError> {
    Ok(FunctionRegister {
        child_functions: None,
        name: FunctionName::try_from(name)?,
        description: "d".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: some(
            dependencies
                .iter()
                .map(|d| TableDependencyDto::try_from(*d))
                .collect::<Result<_, _>>()?,
        ),
        triggers: some(
            triggers
                .iter()
                .map(|t| TableTriggerDto::try_from(*t))
                .collect::<Result<_, _>>()?,
        ),
        tables: some(
            tables
                .iter()
                .map(|t| TableNameDto::try_from(*t))
                .collect::<Result<_, _>>()?,
        ),
        table_types: None,
        parent_tables: None,
        runtime_values: FunctionRuntimeValues::try_from("r")?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: Some(Enabled::from(enabled)),
        workers: None,
    })
}

/// a → {b, c} → d → {e (disabled) → f, g (no tables)}; h is b's child,
/// triggered by b, and i follows h.
async fn seed(db: &TdPool) -> Result<(ProjectTopology, HashMap<String, FunctionId>), TdError> {
    let p = db.seed().test_project().await?;
    let c = db
        .scoped(p.id)
        .seed()
        .collection(&CollectionName::try_from("c")?, &UserId::admin())
        .await?;
    let mut ids = HashMap::new();
    let functions = [
        register("a", &["a_t"], &[], &[], true)?,
        register("b", &["b_t"], &["a_t"], &["a_t"], true)?,
        register("c", &["c_t"], &["a_t"], &[], true)?,
        register("d", &["d_t"], &["b_t", "c_t"], &["b_t", "c_t"], true)?,
        register("e", &["e_t"], &["d_t"], &[], false)?,
        register("f", &["f_t"], &["e_t"], &["e_t"], true)?,
        register("g", &[], &["d_t"], &["d_t"], true)?,
    ];
    for f in &functions {
        let seeded = db.scoped(p.id).seed().function(&c, f).await?;
        ids.insert(f.name.to_string(), seeded.base.function_id);
    }
    let h = db
        .scoped(p.id)
        .seed()
        .child_function(
            &c,
            &register("h", &["h_t"], &["b_t"], &[], true)?,
            &ids["b"],
        )
        .await?;
    ids.insert("h".to_string(), h.base.function_id);
    let i = db
        .scoped(p.id)
        .seed()
        .function(&c, &register("i", &["i_t"], &["h_t"], &["h_t"], true)?)
        .await?;
    ids.insert("i".to_string(), i.base.function_id);

    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut conn = pools.read().await?.enter_project(&p.id).await?;
    let conn = conn.get(Grant::asserted("walk tests"));
    let topology = ProjectTopology::current(conn, &p.id).await?;
    Ok((topology, ids))
}

fn depths(up: Option<i32>, down: Option<i32>) -> RecursionOptions {
    let depth = |d: Option<i32>| d.map(|d| Depth::try_from(d).unwrap());
    RecursionOptions::new(depth(up), depth(down))
}

fn overriding(mut options: RecursionOptions) -> RecursionOptions {
    options.with_override_disabled_trigger(OverrideDisabledTrigger::TRUE);
    options
}

fn named(
    ids: &HashMap<String, FunctionId>,
    ends: impl IntoIterator<Item = (FunctionId, FunctionId)>,
) -> BTreeSet<String> {
    let name: HashMap<_, _> = ids.iter().map(|(n, id)| (*id, n.as_str())).collect();
    ends.into_iter()
        .map(|(from, to)| format!("{}→{}", name[&from], name[&to]))
        .collect()
}

fn triggers<'a>(
    ids: &HashMap<String, FunctionId>,
    t: impl IntoIterator<Item = &'a TopologyTrigger>,
) -> BTreeSet<String> {
    named(
        ids,
        t.into_iter()
            .map(|t| (*t.trigger_by_function_id, t.function_id)),
    )
}

fn dependencies<'a>(
    ids: &HashMap<String, FunctionId>,
    d: impl IntoIterator<Item = &'a TopologyDependency>,
) -> BTreeSet<String> {
    named(
        ids,
        d.into_iter().map(|d| (*d.table_function_id, d.function_id)),
    )
}

fn edges(expected: &[&str]) -> BTreeSet<String> {
    expected.iter().map(|e| e.to_string()).collect()
}

#[td_test::test(db)]
async fn test_trigger_walks(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let walk = |state, start: &str, options: RecursionOptions| {
        let source = TopologyTriggers {
            topology: &topology,
            state,
        };
        triggers(&ids, source.walk(&ids[start], &options))
    };
    let all = [
        "a→b", "a→c", "b→d", "c→d", "d→e", "e→f", "d→g", "b→h", "h→i",
    ];

    assert_eq!(
        walk(FunctionState::Any, "a", depths(Some(0), None)),
        edges(&all)
    );
    assert_eq!(
        walk(FunctionState::Executable, "a", depths(Some(0), None)),
        edges(&["a→b", "a→c", "b→d", "c→d", "d→g", "b→h", "h→i"]),
        "a disabled function stops the walk"
    );
    assert_eq!(
        walk(FunctionState::Any, "a", depths(Some(0), Some(1))),
        edges(&["a→b", "a→c"])
    );
    assert_eq!(
        walk(FunctionState::Any, "a", depths(Some(0), Some(2))),
        edges(&["a→b", "a→c", "b→d", "c→d", "b→h"])
    );
    assert_eq!(
        walk(FunctionState::Any, "d", depths(None, Some(0))),
        edges(&["a→b", "a→c", "b→d", "c→d"])
    );
    assert_eq!(
        walk(FunctionState::Any, "d", depths(Some(1), Some(1))),
        edges(&["b→d", "c→d", "d→e", "d→g"])
    );
    assert_eq!(
        walk(FunctionState::Any, "a", depths(Some(0), Some(0))),
        edges(&[])
    );
    assert_eq!(
        walk(FunctionState::Executable, "e", depths(Some(0), None)),
        edges(&[])
    );
    assert_eq!(
        walk(
            FunctionState::Executable,
            "e",
            overriding(depths(Some(0), None))
        ),
        edges(&["e→f"]),
        "an overridden disabled start walks while active"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_children_run_with_their_parent(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let source = TopologyTriggers {
        topology: &topology,
        state: FunctionState::Any,
    };
    let options = depths(Some(0), Some(0));
    assert_eq!(
        triggers(&ids, source.children_triggers(&ids["b"], &[], &options)),
        edges(&["b→h"])
    );
    let walked = source.walk(&ids["a"], &depths(Some(0), None));
    assert!(
        source
            .children_triggers(&ids["a"], &walked, &options)
            .is_empty(),
        "children the walk reached are not repeated"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_dependency_walks(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let admitted = Admitted::new(&topology, FunctionState::Any, ids["a"]);
    assert_eq!(
        dependencies(
            &ids,
            topology.dependency_walk(&admitted, &depths(Some(0), None))
        ),
        edges(&["a→b", "b→d", "d→g"])
    );
    Ok(())
}

/// A table's flow walks from its writer both ways over every function,
/// functions without tables included.
#[td_test::test(db)]
async fn test_table_flow_walks(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let admitted = Admitted::flow(&topology, ids["d"]);
    let unbounded = depths(None, None);
    assert_eq!(
        triggers(&ids, topology.trigger_walk(&admitted, &unbounded)),
        edges(&["a→b", "a→c", "b→d", "c→d", "d→e", "e→f", "d→g"])
    );
    assert_eq!(
        dependencies(&ids, topology.dependency_walk(&admitted, &unbounded)),
        edges(&["a→b", "b→d", "c→d", "d→g"])
    );
    Ok(())
}

/// Nodes name and group their functions and tables as the catalog views do.
#[td_test::test(db)]
async fn test_nodes_match_the_catalog_views(db: TdPool) -> Result<(), TdError> {
    let (topology, _) = seed(&db).await?;
    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let project = topology.version.base.project_id;
    let mut conn = pools.read().await?.enter_project(&project).await?;
    let conn = conn.get(Grant::asserted("walk tests"));
    let at = &topology.version.interval.valid_from;
    let functions: Vec<FunctionDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
    let tables: Vec<TableDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;

    let graph = topology.graph()?;
    let mut seen = 0;
    for node in graph.inner.node_weights() {
        match node {
            GraphNode::Function(node) => {
                let row = functions
                    .iter()
                    .find(|f| f.base.base.function_id == node.function_id)
                    .unwrap();
                assert_eq!(node.collection, row.collection);
                assert_eq!(node.group_id, row.group_id);
                assert_eq!(node.function_version_id, row.base.base.id);
            }
            GraphNode::Table(node) => {
                let row = tables
                    .iter()
                    .find(|t| t.base.base.table_id == node.table_id)
                    .unwrap();
                assert_eq!(node.collection, row.collection);
                assert_eq!(node.system, row.system);
                assert_eq!(node.table_version_id, row.base.base.id);
            }
        }
        seen += 1;
    }
    assert_eq!(seen, functions.len() + tables.len());
    Ok(())
}
