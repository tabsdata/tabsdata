//
// Copyright 2026 Tabsdata Inc.
//

//! Authorization is owned by the calling service: projects are silos, so the
//! caller gates access with its own project-scoped check.

use crate::topology::walk::Admitted;
use crate::topology::{FunctionState, ProjectTopology};
use crate::{ExecutionPlan, ExecutionScope};
use std::collections::HashSet;
use ta_execution::{PlanScope, TriggerSource};
use td_error::TdError;
use td_objects::dxo::topology::TopologyTrigger;
use td_objects::types::basic::FunctionId;
use td_objects::types::recursion::RecursionOptions;

/// A topology version's triggers between the functions a plan may run.
pub(crate) struct TopologyTriggers<'t> {
    pub topology: &'t ProjectTopology,
    pub state: FunctionState<'t>,
}

impl TriggerSource for TopologyTriggers<'_> {
    type Trigger = TopologyTrigger;

    fn walk(&self, start: &FunctionId, options: &RecursionOptions) -> Vec<&TopologyTrigger> {
        let admitted = Admitted::plan(self.topology, self.state, *start, options);
        self.topology.trigger_walk(&admitted, options)
    }

    fn children_triggers<'a>(
        &'a self,
        start: &FunctionId,
        triggers: &[&'a TopologyTrigger],
        options: &RecursionOptions,
    ) -> Vec<&'a TopologyTrigger> {
        let admitted = Admitted::plan(self.topology, self.state, *start, options);
        self.topology.children_triggers(&admitted, triggers)
    }
}

impl ProjectTopology {
    /// What triggering `start` runs: the triggers this edition plans over
    /// functions in `state`, plus everything they need to run.
    pub fn plan(
        &self,
        state: FunctionState,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> Result<ExecutionPlan, TdError> {
        let trigger = self.function_node(start)?;
        let source = TopologyTriggers {
            topology: self,
            state,
        };
        let triggers = ExecutionScope::plan_triggers(&source, start, options);
        let functions: HashSet<FunctionId> = triggers
            .iter()
            .map(|t| t.function_id)
            .chain([*start])
            .collect();
        let dependencies = self.dependencies_of(&functions);
        let graph = self.subgraph(&triggers, dependencies, &functions)?;
        ExecutionPlan::new(graph, &trigger)
    }
}
