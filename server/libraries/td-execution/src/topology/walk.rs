//
// Copyright 2026 Tabsdata Inc.
//

use crate::topology::{FunctionState, ProjectTopology};
use std::collections::{BTreeSet, HashMap, HashSet};
use td_objects::dxo::topology::{
    TopologyDependency, TopologyFunction, TopologyTable, TopologyTrigger,
};
use td_objects::types::basic::{
    FunctionId, FunctionStatus, TableStatus, TriggerStatus, TriggerVersionId,
};
use td_objects::types::plan::PlanOptions;

/// The functions a walk from `start` may visit.
pub(crate) struct Admitted<'t> {
    functions: &'t HashMap<FunctionId, TopologyFunction>,
    state: FunctionState,
    start: FunctionId,
    start_passes: bool,
    overriding: bool,
}

impl<'t> Admitted<'t> {
    /// A plan's: those in `state`. Overriding a disabled start lets it and
    /// its children through while active.
    pub(crate) fn plan(
        topology: &'t ProjectTopology,
        state: FunctionState,
        start: FunctionId,
        options: &PlanOptions,
    ) -> Self {
        let mut admitted = Self {
            functions: &topology.functions,
            state,
            start,
            start_passes: false,
            overriding: *options.override_disabled_trigger,
        };
        admitted.start_passes = topology
            .functions
            .get(&start)
            .is_some_and(|f| admitted.overrides(f));
        admitted
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
        state: FunctionState,
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

    fn overrides(&self, function: &TopologyFunction) -> bool {
        self.overriding && function.status == FunctionStatus::Active
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
    /// The active triggers from `function`'s tables into functions `admitted`
    /// lets through; none when `function` itself is not.
    pub(crate) fn triggers_from(
        &self,
        admitted: &Admitted,
        function: &FunctionId,
    ) -> Vec<&TopologyTrigger> {
        if !admitted.admit(function) {
            return Vec::new();
        }
        self.triggers_from
            .get(function)
            .into_iter()
            .flatten()
            .map(|&i| &self.triggers[i])
            .filter(|t| t.status == TriggerStatus::Active && admitted.admit(&t.function_id))
            .collect()
    }

    /// The active triggers reachable from the start either way through
    /// `admitted`.
    pub(crate) fn trigger_walk(&self, admitted: &Admitted) -> Vec<&TopologyTrigger> {
        if !admitted.admit(&admitted.start) {
            return Vec::new();
        }
        let keep = |t: &TopologyTrigger, far: &FunctionId| {
            t.status == TriggerStatus::Active && admitted.admit(far)
        };
        let mut kept = reach(
            &self.triggers,
            &self.triggers_from,
            &admitted.start,
            |t| t.function_id,
            keep,
        );
        kept.extend(reach(
            &self.triggers,
            &self.triggers_into,
            &admitted.start,
            |t| *t.trigger_by_function_id,
            keep,
        ));
        kept.into_iter().map(|i| &self.triggers[i]).collect()
    }

    /// The dependencies reachable from the start either way through
    /// `admitted`.
    pub(crate) fn dependency_walk(&self, admitted: &Admitted) -> Vec<&TopologyDependency> {
        if !admitted.admit(&admitted.start) {
            return Vec::new();
        }
        let keep = |_: &TopologyDependency, far: &FunctionId| admitted.admit(far);
        let mut kept = reach(
            &self.dependencies,
            &self.dependencies_from,
            &admitted.start,
            |d| d.function_id,
            keep,
        );
        kept.extend(reach(
            &self.dependencies,
            &self.dependencies_into,
            &admitted.start,
            |d| *d.table_function_id,
            keep,
        ));
        kept.into_iter().map(|i| &self.dependencies[i]).collect()
    }

    /// The active triggers tying the start and the functions in `triggers`
    /// to their children, minus the ones already in `triggers`. Children run
    /// with their parent whatever the plan reaches.
    pub(crate) fn children_triggers<'t>(
        &'t self,
        admitted: &Admitted,
        triggers: &[&TopologyTrigger],
    ) -> Vec<&'t TopologyTrigger> {
        let in_graph: HashSet<FunctionId> = triggers
            .iter()
            .flat_map(|t| [*t.trigger_by_function_id, t.function_id])
            .chain([admitted.start])
            .collect();
        let children: HashSet<FunctionId> = self
            .functions
            .values()
            .filter(|f| {
                self.tags.parent(&f.function_id).is_some_and(|parent| {
                    (in_graph.contains(parent) && admitted.state.matches(f))
                        || (*parent == admitted.start && admitted.overrides(f))
                })
            })
            .map(|f| f.function_id)
            .collect();
        let known: HashSet<&TriggerVersionId> = triggers.iter().map(|t| &t.id).collect();
        self.triggers
            .iter()
            .filter(|t| t.status == TriggerStatus::Active)
            .filter(|t| children.contains(&t.function_id))
            .filter(|t| in_graph.contains(&*t.trigger_by_function_id))
            .filter(|t| !known.contains(&t.id))
            .collect()
    }

    pub(crate) fn dependencies_of(
        &self,
        functions: &HashSet<FunctionId>,
    ) -> Vec<&TopologyDependency> {
        self.dependencies
            .iter()
            .filter(|d| functions.contains(&d.function_id))
            .collect()
    }

    /// The active tables `functions` write.
    pub(crate) fn outputs_of(&self, functions: &HashSet<FunctionId>) -> Vec<&TopologyTable> {
        self.ordered_tables()
            .filter(|t| t.status == TableStatus::Active)
            .filter(|t| functions.contains(&t.function_id))
            .collect()
    }
}

/// The edges reachable from `start`, each kept by `keep` with its far end,
/// in load order.
fn reach<E>(
    edges: &[E],
    adjacent: &HashMap<FunctionId, Vec<usize>>,
    start: &FunctionId,
    far: impl Fn(&E) -> FunctionId,
    keep: impl Fn(&E, &FunctionId) -> bool,
) -> BTreeSet<usize> {
    let mut kept = BTreeSet::new();
    let mut seen = HashSet::from([*start]);
    let mut frontier = vec![*start];
    while let Some(node) = frontier.pop() {
        for &i in adjacent.get(&node).into_iter().flatten() {
            let end = far(&edges[i]);
            if keep(&edges[i], &end) {
                kept.insert(i);
                if seen.insert(end) {
                    frontier.push(end);
                }
            }
        }
    }
    kept
}
