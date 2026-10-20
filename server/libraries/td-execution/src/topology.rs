//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::{FunctionState, TdGraph, TdGraphBuilder};
use std::collections::{BTreeSet, HashMap, HashSet};
use td_database::access::{AccessMode, InProject};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::topology::ProjectTopologyDB;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{
    Depth, FunctionId, FunctionStatus, ProjectId, TableId, TableStatus, TriggerStatus,
    TriggerVersionId,
};
use td_objects::types::recursion::RecursionOptions;
use td_queries::Queries;

/// A project's whole catalog at one topology version, indexed for walks.
pub struct ProjectTopology {
    pub version: ProjectTopologyDB,
    pub(crate) functions: HashMap<FunctionId, FunctionDBWithNames>,
    pub(crate) tables: HashMap<TableId, TableDBWithNames>,
    table_order: Vec<TableId>,
    triggers: Vec<TriggerDBWithNames>,
    dependencies: Vec<DependencyDBWithNames>,
    triggers_from: HashMap<FunctionId, Vec<usize>>,
    triggers_into: HashMap<FunctionId, Vec<usize>>,
    dependencies_from: HashMap<FunctionId, Vec<usize>>,
    dependencies_into: HashMap<FunctionId, Vec<usize>>,
}

/// The functions a walk may visit: those in the [`FunctionState`], plus the
/// start when `start_passes`.
struct Nodes<'t> {
    functions: &'t HashMap<FunctionId, FunctionDBWithNames>,
    state: FunctionState<'t>,
    start: FunctionId,
    start_passes: bool,
}

impl Nodes<'_> {
    fn admit(&self, function: &FunctionId) -> bool {
        (self.start_passes && *function == self.start)
            || self
                .functions
                .get(function)
                .is_some_and(|f| self.state.matches(f))
    }
}

impl ProjectTopology {
    /// The project's open version.
    pub async fn current<M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, InProject, M>,
        project: &ProjectId,
    ) -> Result<Self, TdError> {
        let version: ProjectTopologyDB = conn.scd2().fetch_one(project).await?;
        Self::of(conn, version).await
    }

    pub async fn of<M: AccessMode>(
        conn: &mut DomainConnection<Apiserver, InProject, M>,
        version: ProjectTopologyDB,
    ) -> Result<Self, TdError> {
        let at = &version.interval.valid_from;
        let functions: Vec<FunctionDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
        let mut tables: Vec<TableDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
        let mut dependencies: Vec<DependencyDBWithNames> =
            conn.scd2().as_of(at).fetch_all(()).await?;
        let mut triggers: Vec<TriggerDBWithNames> = conn.scd2().as_of(at).fetch_all(()).await?;
        // Graphs insert nodes in this order, and `dot` numbers them by it.
        tables.sort_by_key(|t| t.base.base.table_id);
        dependencies.sort_by_key(|d| d.base.base.id);
        triggers.sort_by_key(|t| t.base.base.id);

        Ok(Self {
            triggers_from: index(triggers.iter().map(|t| *t.base.base.trigger_by_function_id)),
            triggers_into: index(triggers.iter().map(|t| t.base.base.function_id)),
            dependencies_from: index(dependencies.iter().map(|d| *d.base.base.table_function_id)),
            dependencies_into: index(dependencies.iter().map(|d| d.base.base.function_id)),
            table_order: tables.iter().map(|t| t.base.base.table_id).collect(),
            tables: tables
                .into_iter()
                .map(|t| (t.base.base.table_id, t))
                .collect(),
            functions: functions
                .into_iter()
                .map(|f| (f.base.base.function_id, f))
                .collect(),
            triggers,
            dependencies,
            version,
        })
    }

    /// The whole project graph.
    pub fn graph(&self) -> Result<TdGraph, TdError> {
        // A frozen table outlives the function that wrote it: no output edge.
        let outputs = self
            .ordered_tables()
            .filter(|t| self.functions.contains_key(&t.base.base.function_id));
        Ok(TdGraphBuilder::new(&self.tables, &self.functions)
            .with_triggers(&self.triggers)?
            .with_dependencies(&self.dependencies)?
            .with_outputs(outputs)?
            .build())
    }

    /// The active triggers reachable from `start` over functions in `state`.
    pub(crate) fn trigger_walk(
        &self,
        start: &FunctionId,
        start_passes: bool,
        state: FunctionState,
        options: &RecursionOptions,
    ) -> Vec<&TriggerDBWithNames> {
        let nodes = Nodes {
            functions: &self.functions,
            state,
            start: *start,
            start_passes,
        };
        if !nodes.admit(start) {
            return Vec::new();
        }
        let keep = |t: &TriggerDBWithNames, far: &FunctionId| {
            t.base.base.status == TriggerStatus::Active && nodes.admit(far)
        };
        let mut kept = reach(
            &self.triggers,
            &self.triggers_from,
            start,
            options.max_downstream_depth(),
            |t| t.base.base.function_id,
            keep,
        );
        kept.extend(reach(
            &self.triggers,
            &self.triggers_into,
            start,
            options.max_upstream_depth(),
            |t| *t.base.base.trigger_by_function_id,
            keep,
        ));
        kept.into_iter().map(|i| &self.triggers[i]).collect()
    }

    /// The dependencies reachable from `start` over functions in `state`.
    pub(crate) fn dependency_walk(
        &self,
        start: &FunctionId,
        start_passes: bool,
        state: FunctionState,
        options: &RecursionOptions,
    ) -> Vec<&DependencyDBWithNames> {
        let nodes = Nodes {
            functions: &self.functions,
            state,
            start: *start,
            start_passes,
        };
        if !nodes.admit(start) {
            return Vec::new();
        }
        let keep = |_: &DependencyDBWithNames, far: &FunctionId| nodes.admit(far);
        let mut kept = reach(
            &self.dependencies,
            &self.dependencies_from,
            start,
            options.max_downstream_depth(),
            |d| d.base.base.function_id,
            keep,
        );
        kept.extend(reach(
            &self.dependencies,
            &self.dependencies_into,
            start,
            options.max_upstream_depth(),
            |d| *d.base.base.table_function_id,
            keep,
        ));
        kept.into_iter().map(|i| &self.dependencies[i]).collect()
    }

    /// Children run with their parent whatever the walk depth, and follow an
    /// override of a disabled start.
    pub(crate) fn children_triggers<'t>(
        &'t self,
        start: &FunctionId,
        triggers: &[&TriggerDBWithNames],
        state: FunctionState,
        options: &RecursionOptions,
    ) -> Vec<&'t TriggerDBWithNames> {
        let in_graph: HashSet<FunctionId> = triggers
            .iter()
            .flat_map(|t| [*t.base.base.trigger_by_function_id, t.base.base.function_id])
            .chain([*start])
            .collect();
        let overridden = options.override_disabled_trigger();
        let children: HashSet<FunctionId> = self
            .functions
            .values()
            .filter(|f| {
                f.base
                    .base
                    .parent_function_id
                    .as_ref()
                    .is_some_and(|parent| {
                        (in_graph.contains(&**parent) && state.matches(f))
                            || (overridden
                                && **parent == *start
                                && f.base.base.status == FunctionStatus::Active)
                    })
            })
            .map(|f| f.base.base.function_id)
            .collect();
        let known: HashSet<&TriggerVersionId> = triggers.iter().map(|t| &t.base.base.id).collect();
        self.triggers
            .iter()
            .filter(|t| t.base.base.status == TriggerStatus::Active)
            .filter(|t| children.contains(&t.base.base.function_id))
            .filter(|t| in_graph.contains(&*t.base.base.trigger_by_function_id))
            .filter(|t| !known.contains(&t.base.base.id))
            .collect()
    }

    /// The dependencies of `functions`.
    pub(crate) fn dependencies_of(
        &self,
        functions: &HashSet<FunctionId>,
    ) -> Vec<&DependencyDBWithNames> {
        self.dependencies
            .iter()
            .filter(|d| functions.contains(&d.base.base.function_id))
            .collect()
    }

    /// The active tables `functions` write.
    pub(crate) fn outputs_of(&self, functions: &HashSet<FunctionId>) -> Vec<&TableDBWithNames> {
        self.ordered_tables()
            .filter(|t| t.base.base.status == TableStatus::Active)
            .filter(|t| functions.contains(&t.base.base.function_id))
            .collect()
    }

    fn ordered_tables(&self) -> impl Iterator<Item = &TableDBWithNames> {
        self.table_order.iter().map(|id| &self.tables[id])
    }
}

/// Edge positions by the function at one end.
fn index(ends: impl Iterator<Item = FunctionId>) -> HashMap<FunctionId, Vec<usize>> {
    let mut index: HashMap<FunctionId, Vec<usize>> = HashMap::new();
    for (i, end) in ends.enumerate() {
        index.entry(end).or_default().push(i);
    }
    index
}

/// The edges reachable from `start` in at most `max_depth` hops (`None`
/// unbounded, zero none), each kept by `keep` with its far end, in load order.
fn reach<E>(
    edges: &[E],
    adjacent: &HashMap<FunctionId, Vec<usize>>,
    start: &FunctionId,
    max_depth: Option<Depth>,
    far: impl Fn(&E) -> FunctionId,
    keep: impl Fn(&E, &FunctionId) -> bool,
) -> BTreeSet<usize> {
    let mut kept = BTreeSet::new();
    let mut seen = HashSet::from([*start]);
    let mut frontier = vec![*start];
    let mut depth = 0;
    while !frontier.is_empty() && max_depth.is_none_or(|max| depth < *max) {
        let mut next = Vec::new();
        for node in frontier {
            for &i in adjacent.get(&node).into_iter().flatten() {
                let end = far(&edges[i]);
                if keep(&edges[i], &end) {
                    kept.insert(i);
                    if seen.insert(end) {
                        next.push(end);
                    }
                }
            }
        }
        frontier = next;
        depth += 1;
    }
    kept
}
