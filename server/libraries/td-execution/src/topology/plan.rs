//
// Copyright 2026 Tabsdata Inc.
//

//! Authorization is owned by the calling service: projects are silos, so the
//! caller gates access with its own project-scoped check.

use crate::ExecutionScope;
use crate::graph::{TdGraph, TdGraphBuilder};
use crate::topology::walk::Nodes;
use crate::topology::{FunctionState, ProjectTopology};
use std::collections::HashSet;
use ta_execution::{GraphSource, PlanScope};
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::FunctionId;
use td_objects::types::recursion::RecursionOptions;

/// A topology version seen through the functions a plan may run.
pub(crate) struct PlanSource<'t> {
    pub topology: &'t ProjectTopology,
    pub state: FunctionState<'t>,
}

impl GraphSource for PlanSource<'_> {
    fn walk(&self, start: &FunctionId, options: &RecursionOptions) -> Vec<&TriggerDBWithNames> {
        let nodes = Nodes::plan(self.topology, self.state, *start, options);
        self.topology.trigger_walk(&nodes, options)
    }

    fn children_triggers<'a>(
        &'a self,
        start: &FunctionId,
        triggers: &[&'a TriggerDBWithNames],
        options: &RecursionOptions,
    ) -> Vec<&'a TriggerDBWithNames> {
        let nodes = Nodes::plan(self.topology, self.state, *start, options);
        self.topology.children_triggers(&nodes, triggers)
    }
}

impl ProjectTopology {
    /// The execution plan graph of `start`: the triggers this edition plans
    /// over functions in `state`, plus everything they need to run.
    pub fn plan_graph(
        &self,
        state: FunctionState,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> Result<TdGraph, TdError> {
        let source = PlanSource {
            topology: self,
            state,
        };
        let triggers = ExecutionScope::plan_triggers(&source, start, options);
        let functions: HashSet<FunctionId> = triggers
            .iter()
            .map(|t| t.base.base.function_id)
            .chain([*start])
            .collect();
        let dependencies = self.dependencies_of(&functions);
        self.subgraph(&triggers, dependencies, &functions)
    }

    /// Everything flowing into and out of `table`, whatever the state of the
    /// functions on the way.
    pub fn flow_graph(&self, table: &TableDBWithNames) -> Result<TdGraph, TdError> {
        let start = table.base.base.function_id;
        let nodes = Nodes::flow(self, start);
        let unbounded = RecursionOptions::new(None, None);
        let mut triggers = self.trigger_walk(&nodes, &unbounded);
        let children = self.children_triggers(&nodes, &triggers);
        triggers.extend(children);
        let dependencies = self.dependency_walk(&nodes, &unbounded);
        let functions: HashSet<FunctionId> = triggers
            .iter()
            .flat_map(|t| [*t.base.base.trigger_by_function_id, t.base.base.function_id])
            .chain(
                dependencies
                    .iter()
                    .flat_map(|d| [*d.base.base.table_function_id, d.base.base.function_id]),
            )
            .chain([start])
            .collect();
        self.subgraph(&triggers, dependencies, &functions)
    }

    /// `triggers` and `dependencies`, plus the active tables `functions` write.
    fn subgraph(
        &self,
        triggers: &[&TriggerDBWithNames],
        dependencies: Vec<&DependencyDBWithNames>,
        functions: &HashSet<FunctionId>,
    ) -> Result<TdGraph, TdError> {
        Ok(TdGraphBuilder::new(&self.tables, &self.functions)
            .with_triggers(triggers.iter().copied())?
            .with_dependencies(dependencies)?
            .with_outputs(self.outputs_of(functions))?
            .build())
    }
}
