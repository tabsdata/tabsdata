//
// Copyright 2026 Tabsdata Inc.
//

//! The walks over one seeded topology, edge by edge.

use crate::topology::walk::Admitted;
use crate::topology::{FunctionState, ProjectTopology};
use std::collections::{BTreeSet, HashMap};
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
use td_objects::dxo::graph::GraphNode;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::topology::{TopologyDependency, TopologyTrigger};
use td_objects::types::basic::{
    BundleId, CollectionName, Decorator, Enabled, FunctionId, FunctionName, FunctionOnly,
    FunctionRuntimeValues, OverrideDisabledTrigger, ReuseFrozen, TableNameDto, UserId,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_objects::types::plan::PlanOptions;
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

fn options(override_disabled_trigger: bool) -> PlanOptions {
    PlanOptions {
        function_only: FunctionOnly::FALSE,
        override_disabled_trigger: OverrideDisabledTrigger::from(override_disabled_trigger),
    }
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

/// The triggers out of one function, as a plan sees them: active, into
/// functions the plan may run, and none out of a function it may not.
#[td_test::test(db)]
async fn test_triggers_from(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let from = |state, start: &str, overriding: bool, function: &str| {
        let admitted = Admitted::plan(&topology, state, ids[start], &options(overriding));
        triggers(&ids, topology.triggers_from(&admitted, &ids[function]))
    };
    assert_eq!(
        from(FunctionState::Any, "a", false, "a"),
        edges(&["a→b", "a→c"])
    );
    assert_eq!(
        from(FunctionState::Any, "a", false, "d"),
        edges(&["d→e", "d→g"])
    );
    assert_eq!(
        from(FunctionState::Executable, "a", false, "d"),
        edges(&["d→g"]),
        "a disabled function is not triggered"
    );
    assert_eq!(
        from(FunctionState::Executable, "e", false, "e"),
        edges(&[]),
        "a disabled start triggers nothing"
    );
    assert_eq!(
        from(FunctionState::Executable, "e", true, "e"),
        edges(&["e→f"]),
        "an overridden disabled start triggers while active"
    );
    Ok(())
}

#[td_test::test(db)]
async fn test_children_run_with_their_parent(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let from_b = Admitted::plan(&topology, FunctionState::Any, ids["b"], &options(false));
    assert_eq!(
        triggers(&ids, topology.children_triggers(&from_b, &[])),
        edges(&["b→h"])
    );
    let from_a = Admitted::plan(&topology, FunctionState::Any, ids["a"], &options(false));
    let walked = topology.trigger_walk(&from_a);
    assert!(
        topology.children_triggers(&from_a, &walked).is_empty(),
        "children the walk reached are not repeated"
    );
    Ok(())
}

/// A walk goes both ways over the functions it admits.
#[td_test::test(db)]
async fn test_walks(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let all = [
        "a→b", "a→c", "b→d", "c→d", "d→e", "e→f", "d→g", "b→h", "h→i",
    ];
    let walk = |state, start: &str| {
        let admitted = Admitted::new(&topology, state, ids[start]);
        triggers(&ids, topology.trigger_walk(&admitted))
    };
    assert_eq!(walk(FunctionState::Any, "a"), edges(&all));
    assert_eq!(
        walk(FunctionState::Any, "g"),
        edges(&["a→b", "a→c", "b→d", "c→d", "d→g"]),
        "upstream, a walk follows triggers into what it reached, not out of it"
    );
    assert_eq!(
        walk(FunctionState::Executable, "a"),
        edges(&["a→b", "a→c", "b→d", "c→d", "d→g", "b→h", "h→i"]),
        "a disabled function stops the walk"
    );
    assert_eq!(walk(FunctionState::Executable, "e"), edges(&[]));

    let admitted = Admitted::new(&topology, FunctionState::Any, ids["a"]);
    assert_eq!(
        dependencies(&ids, topology.dependency_walk(&admitted)),
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
    assert_eq!(
        triggers(&ids, topology.trigger_walk(&admitted)),
        edges(&["a→b", "a→c", "b→d", "c→d", "d→e", "e→f", "d→g"])
    );
    assert_eq!(
        dependencies(&ids, topology.dependency_walk(&admitted)),
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
