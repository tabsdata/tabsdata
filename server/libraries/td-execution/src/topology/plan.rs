//
// Copyright 2026 Tabsdata Inc.
//

//! Authorization is owned by the calling service: projects are silos, so the
//! caller gates access with its own project-scoped check.

use crate::topology::walk::Admitted;
use crate::topology::{FunctionState, ProjectTopology};
use crate::{ExecutionPlan, ExecutionScope};
use std::collections::{HashMap, HashSet};
use ta_execution::{PlanGraph, PlanScope};
use td_error::{TdError, td_error};
use td_objects::dxo::topology::{TopologyDependency, TopologyTrigger};
use td_objects::types::basic::{DependencyPos, FunctionId, TableId};
use td_objects::types::plan::PlanOptions;

#[td_error]
enum RecordedError {
    #[error("No trigger of function '{0}' by table '{1}' in the plan's topology version")]
    Trigger(FunctionId, TableId) = 5000,
    #[error("No dependency of function '{0}' at position '{1}' in the plan's topology version")]
    Dependency(FunctionId, DependencyPos) = 5001,
}

/// A topology version seen through the functions a plan may run.
pub(crate) struct AdmittedTopology<'t> {
    pub topology: &'t ProjectTopology,
    pub admitted: Admitted<'t>,
}

impl PlanGraph for AdmittedTopology<'_> {
    type Trigger = TopologyTrigger;

    fn triggers_from(&self, function: &FunctionId) -> Vec<&TopologyTrigger> {
        self.topology.triggers_from(&self.admitted, function)
    }

    fn triggered(&self, trigger: &TopologyTrigger) -> FunctionId {
        trigger.function_id
    }

    fn children_triggers<'a>(
        &'a self,
        triggers: &[&'a TopologyTrigger],
    ) -> Vec<&'a TopologyTrigger> {
        self.topology.children_triggers(&self.admitted, triggers)
    }
}

impl ProjectTopology {
    /// What triggering `start` runs: the triggers this edition plans over
    /// functions in `state`, plus everything they need to run.
    pub fn plan(
        &self,
        state: FunctionState,
        start: &FunctionId,
        options: &PlanOptions,
    ) -> Result<ExecutionPlan, TdError> {
        let trigger = self.function_node(start)?;
        let graph = AdmittedTopology {
            topology: self,
            admitted: Admitted::plan(self, state, *start, options),
        };
        let triggers = ExecutionScope::plan_triggers(&graph, start, options);
        let functions: HashSet<FunctionId> = triggers
            .iter()
            .map(|t| t.function_id)
            .chain([*start])
            .collect();
        let dependencies = self.dependencies_of(&functions);
        let graph = self.subgraph(triggers, dependencies, &functions)?;
        ExecutionPlan::new(graph, &trigger)
    }

    /// The plan an execution recorded: `triggers` as (triggered function, by
    /// table) and `dependencies` as (function, position), each a row of this
    /// topology version.
    pub fn recorded(
        &self,
        start: &FunctionId,
        triggers: &[(FunctionId, TableId)],
        dependencies: &[(FunctionId, DependencyPos)],
    ) -> Result<ExecutionPlan, TdError> {
        let trigger = self.function_node(start)?;
        let by_table: HashMap<(FunctionId, TableId), &TopologyTrigger> = self
            .triggers
            .iter()
            .map(|t| ((t.function_id, t.trigger_by_table_id), t))
            .collect();
        let by_position: HashMap<(FunctionId, DependencyPos), &TopologyDependency> = self
            .dependencies
            .iter()
            .map(|d| ((d.function_id, d.dep_pos), d))
            .collect();
        let triggers = triggers
            .iter()
            .map(|key| {
                by_table
                    .get(key)
                    .copied()
                    .ok_or_else(|| RecordedError::Trigger(key.0, key.1).into())
            })
            .collect::<Result<Vec<_>, TdError>>()?;
        let dependencies = dependencies
            .iter()
            .map(|key| {
                by_position
                    .get(key)
                    .copied()
                    .ok_or_else(|| RecordedError::Dependency(key.0, key.1).into())
            })
            .collect::<Result<Vec<_>, TdError>>()?;
        let functions: HashSet<FunctionId> = triggers
            .iter()
            .map(|t| t.function_id)
            .chain([*start])
            .collect();
        let graph = self.subgraph(triggers, dependencies, &functions)?;
        ExecutionPlan::new(graph, &trigger)
    }
}
