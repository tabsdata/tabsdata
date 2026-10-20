//
// Copyright 2026 Tabsdata Inc.
//

//! The in-memory walks visit what the recursive SQL walks did, except that a
//! table-centered walk also reaches functions without tables.

use crate::topology::plan::PlanSource;
use crate::topology::walk::Nodes;
use crate::topology::{FunctionState, ProjectTopology};
use std::collections::{HashMap, HashSet};
use ta_execution::GraphSource;
use td_database::access::{AcrossProjects, DbPool, Grant};
use td_database::domain::DomainPools;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::table::TableDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::sql_condition::SqlCondition;
use td_objects::types::basic::{
    BundleId, CollectionName, Decorator, DependencyVersionId, Depth, Enabled, FunctionId,
    FunctionName, FunctionRuntimeValues, OverrideDisabledTrigger, ReuseFrozen, TableNameDto,
    TriggerVersionId, UserId,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_objects::types::recursion::RecursionOptions;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const STATES: [FunctionState; 2] = [FunctionState::Any, FunctionState::Executable];

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

fn options() -> Vec<RecursionOptions> {
    let depth = |d: Option<i32>| d.map(|d| Depth::try_from(d).unwrap());
    let depths = [
        (Some(0), Some(0)),
        (Some(0), None),
        (Some(0), Some(1)),
        (None, Some(0)),
        (Some(1), Some(1)),
        (Some(2), Some(2)),
        (None, None),
    ];
    let mut all = Vec::new();
    for (up, down) in depths {
        for overridden in [false, true] {
            let mut options = RecursionOptions::new(depth(up), depth(down));
            options.with_override_disabled_trigger(OverrideDisabledTrigger::from(overridden));
            all.push(options);
        }
    }
    all
}

fn condition(state: FunctionState) -> Box<dyn SqlCondition<FunctionDB>> {
    match state {
        FunctionState::Any => Box::new(()),
        FunctionState::Ran(_) => unreachable!(),
        FunctionState::Executable => Box::new(FunctionDB::executable()),
    }
}

fn trigger_ids<'a>(
    t: impl IntoIterator<Item = &'a TriggerDBWithNames>,
) -> HashSet<TriggerVersionId> {
    t.into_iter().map(|t| t.base.base.id).collect()
}

fn dependency_ids<'a>(
    d: impl IntoIterator<Item = &'a DependencyDBWithNames>,
) -> HashSet<DependencyVersionId> {
    d.into_iter().map(|d| d.base.base.id).collect()
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

#[td_test::test(db)]
async fn test_function_walks_match_the_sql_walks(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let project = topology.version.base.project_id;
    let at = topology.version.interval.valid_from;
    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = pools.read().await?.enter_project(&project).await?;
    let conn = trx.get(Grant::asserted("walk tests"));
    let mut compared = 0;
    for (name, function) in &ids {
        for options in options() {
            for state in STATES {
                let cond = condition(state);
                let walk = conn
                    .scd2()
                    .as_of(&at)
                    .recursive(options)
                    .start_at(function)
                    .where_edges(TriggerDBWithNames::active())
                    .where_nodes(&cond);
                let walk = if options.override_disabled_trigger() {
                    walk.where_start(FunctionDB::active())
                } else {
                    walk
                };
                let sql: Vec<TriggerDBWithNames> = walk.fetch_all().await?;
                let source = PlanSource {
                    topology: &topology,
                    state,
                };
                let memory = source.walk(function, &options);
                assert_eq!(
                    trigger_ids(memory),
                    trigger_ids(&sql),
                    "triggers from {name}, {options:?}, {state:?}"
                );

                let sql: Vec<DependencyDBWithNames> = conn
                    .scd2()
                    .as_of(&at)
                    .recursive(options)
                    .start_at(function)
                    .where_nodes(&cond)
                    .fetch_all()
                    .await?;
                let memory =
                    topology.dependency_walk(&Nodes::new(&topology, state, *function), &options);
                assert_eq!(
                    dependency_ids(memory),
                    dependency_ids(&sql),
                    "dependencies from {name}, {options:?}, {state:?}"
                );
                compared += 1;
            }
        }
    }
    assert_eq!(compared, ids.len() * options().len() * STATES.len());

    let everything = RecursionOptions::new(None, None);
    let reached = |state| {
        PlanSource {
            topology: &topology,
            state,
        }
        .walk(&ids["a"], &everything)
        .len()
    };
    assert_eq!(reached(FunctionState::Any), 9);
    assert_eq!(reached(FunctionState::Executable), 7);
    Ok(())
}

#[td_test::test(db)]
async fn test_table_walks_also_reach_functions_without_tables(db: TdPool) -> Result<(), TdError> {
    let (topology, ids) = seed(&db).await?;
    let project = topology.version.base.project_id;
    let at = topology.version.interval.valid_from;
    let pools =
        DbPool::<AcrossProjects>::new(DomainPools::of_apiserver(db.clone()), AcrossProjects);
    let mut trx = pools.read().await?.enter_project(&project).await?;
    let conn = trx.get(Grant::asserted("walk tests"));
    let tableless = ids["g"];
    let touches_tableless = |from: FunctionId, to: FunctionId| from == tableless || to == tableless;
    let mut extra_triggers = 0;
    for table in topology.tables.values() {
        let table_id = table.base.base.table_id;
        let function = table.base.base.function_id;
        for options in options() {
            let sql: Vec<TriggerDBWithNames> = conn
                .scd2()
                .as_of(&at)
                .recursive::<_, TableDB>(options)
                .start_at(&table_id)
                .where_edges(TriggerDBWithNames::active())
                .fetch_all()
                .await?;
            let memory = topology.trigger_walk(&Nodes::flow(&topology, function), &options);
            let sql_ids = trigger_ids(&sql);
            for t in &memory {
                if !sql_ids.contains(&t.base.base.id) {
                    assert!(touches_tableless(
                        *t.base.base.trigger_by_function_id,
                        t.base.base.function_id
                    ));
                    extra_triggers += 1;
                }
            }
            assert_eq!(
                trigger_ids(memory.iter().copied().filter(|t| !touches_tableless(
                    *t.base.base.trigger_by_function_id,
                    t.base.base.function_id
                ))),
                trigger_ids(&sql),
                "triggers from {}, {options:?}",
                table.base.base.name
            );

            let sql: Vec<DependencyDBWithNames> = conn
                .scd2()
                .as_of(&at)
                .recursive::<_, TableDB>(options)
                .start_at(&table_id)
                .fetch_all()
                .await?;
            let memory = topology.dependency_walk(&Nodes::flow(&topology, function), &options);
            assert_eq!(
                dependency_ids(memory.iter().copied().filter(|d| !touches_tableless(
                    *d.base.base.table_function_id,
                    d.base.base.function_id
                ))),
                dependency_ids(&sql),
                "dependencies from {}, {options:?}",
                table.base.base.name
            );
        }
    }
    assert!(extra_triggers > 0);
    Ok(())
}
