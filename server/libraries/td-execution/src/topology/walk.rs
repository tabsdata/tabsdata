//
// Copyright 2026 Tabsdata Inc.
//

use crate::topology::{FunctionState, ProjectTopology};
use std::collections::{BTreeSet, HashMap, HashSet};
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{
    Depth, FunctionId, FunctionStatus, TableStatus, TriggerStatus, TriggerVersionId,
};
use td_objects::types::recursion::RecursionOptions;

/// The functions a walk from `start` may visit.
pub(crate) struct Nodes<'t> {
    functions: &'t HashMap<FunctionId, FunctionDBWithNames>,
    state: FunctionState<'t>,
    start: FunctionId,
    start_passes: bool,
    overriding: bool,
}

impl<'t> Nodes<'t> {
    /// A plan's: those in `state`. Overriding a disabled start lets it and
    /// its children through while active.
    pub(crate) fn plan(
        topology: &'t ProjectTopology,
        state: FunctionState<'t>,
        start: FunctionId,
        options: &RecursionOptions,
    ) -> Self {
        let mut nodes = Self {
            functions: &topology.functions,
            state,
            start,
            start_passes: false,
            overriding: options.override_disabled_trigger(),
        };
        nodes.start_passes = topology
            .functions
            .get(&start)
            .is_some_and(|f| nodes.overrides(f));
        nodes
    }

    /// A table flow's: every function, from the table's writer even once a
    /// frozen table outlives it.
    pub(crate) fn flow(topology: &'t ProjectTopology, start: FunctionId) -> Self {
        Self {
            functions: &topology.functions,
            state: FunctionState::Any,
            start,
            start_passes: true,
            overriding: false,
        }
    }

    /// Only the functions in `state`, the start included.
    #[cfg(test)]
    pub(crate) fn new(
        topology: &'t ProjectTopology,
        state: FunctionState<'t>,
        start: FunctionId,
    ) -> Self {
        Self {
            functions: &topology.functions,
            state,
            start,
            start_passes: false,
            overriding: false,
        }
    }

    fn overrides(&self, function: &FunctionDBWithNames) -> bool {
        self.overriding && function.base.base.status == FunctionStatus::Active
    }

    fn admit(&self, function: &FunctionId) -> bool {
        (self.start_passes && *function == self.start)
            || self
                .functions
                .get(function)
                .is_some_and(|f| self.state.matches(f))
    }
}

impl ProjectTopology {
    /// The active triggers reachable from the start through `nodes`.
    pub(crate) fn trigger_walk(
        &self,
        nodes: &Nodes,
        options: &RecursionOptions,
    ) -> Vec<&TriggerDBWithNames> {
        if !nodes.admit(&nodes.start) {
            return Vec::new();
        }
        let keep = |t: &TriggerDBWithNames, far: &FunctionId| {
            t.base.base.status == TriggerStatus::Active && nodes.admit(far)
        };
        let mut kept = reach(
            &self.triggers,
            &self.triggers_from,
            &nodes.start,
            options.max_downstream_depth(),
            |t| t.base.base.function_id,
            keep,
        );
        kept.extend(reach(
            &self.triggers,
            &self.triggers_into,
            &nodes.start,
            options.max_upstream_depth(),
            |t| *t.base.base.trigger_by_function_id,
            keep,
        ));
        kept.into_iter().map(|i| &self.triggers[i]).collect()
    }

    /// The dependencies reachable from the start through `nodes`.
    pub(crate) fn dependency_walk(
        &self,
        nodes: &Nodes,
        options: &RecursionOptions,
    ) -> Vec<&DependencyDBWithNames> {
        if !nodes.admit(&nodes.start) {
            return Vec::new();
        }
        let keep = |_: &DependencyDBWithNames, far: &FunctionId| nodes.admit(far);
        let mut kept = reach(
            &self.dependencies,
            &self.dependencies_from,
            &nodes.start,
            options.max_downstream_depth(),
            |d| d.base.base.function_id,
            keep,
        );
        kept.extend(reach(
            &self.dependencies,
            &self.dependencies_into,
            &nodes.start,
            options.max_upstream_depth(),
            |d| *d.base.base.table_function_id,
            keep,
        ));
        kept.into_iter().map(|i| &self.dependencies[i]).collect()
    }

    /// The active triggers tying the start and the functions in `triggers`
    /// to their children, minus the ones already in `triggers`. Children run
    /// with their parent whatever the walk depth.
    pub(crate) fn children_triggers<'t>(
        &'t self,
        nodes: &Nodes,
        triggers: &[&TriggerDBWithNames],
    ) -> Vec<&'t TriggerDBWithNames> {
        let in_graph: HashSet<FunctionId> = triggers
            .iter()
            .flat_map(|t| [*t.base.base.trigger_by_function_id, t.base.base.function_id])
            .chain([nodes.start])
            .collect();
        let children: HashSet<FunctionId> = self
            .functions
            .values()
            .filter(|f| {
                f.base
                    .base
                    .parent_function_id
                    .as_ref()
                    .is_some_and(|parent| {
                        (in_graph.contains(&**parent) && nodes.state.matches(f))
                            || (**parent == nodes.start && nodes.overrides(f))
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
