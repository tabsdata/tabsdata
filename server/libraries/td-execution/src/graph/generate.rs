//
// Copyright 2025 Tabsdata Inc.
//

use crate::ExecutionScope;
use crate::graph::source::PlanSource;
use crate::graph::{TdGraph, TdGraphBuilder};
use crate::topology::ProjectTopology;
use std::collections::HashSet;
use ta_execution::PlanScope;
use td_error::TdError;
use td_objects::dxo::dependency::DependencyDBWithNames;
use td_objects::dxo::function::FunctionDBWithNames;
use td_objects::dxo::table::TableDBWithNames;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, FunctionStatus};
use td_objects::types::recursion::RecursionOptions;

/// The functions a walk goes through.
#[derive(Clone, Copy, Debug)]
pub enum FunctionState {
    Any,
    Active,
    /// Active and enabled.
    Executable,
}

impl FunctionState {
    pub fn matches(&self, function: &FunctionDBWithNames) -> bool {
        let function = &function.base.base;
        match self {
            FunctionState::Any => true,
            FunctionState::Active => function.status == FunctionStatus::Active,
            FunctionState::Executable => {
                function.status == FunctionStatus::Active && *function.enabled
            }
        }
    }
}

/// Authorization is owned by the calling service: projects are silos, so the
/// caller gates access with its own project-scoped check.
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
    /// functions on the way: the walks start at its writer, even when a
    /// frozen table outlives it.
    pub fn flow_graph(&self, table: &TableDBWithNames) -> Result<TdGraph, TdError> {
        let start = table.base.base.function_id;
        let unbounded = RecursionOptions::new(None, None);
        let mut triggers = self.trigger_walk(&start, true, FunctionState::Any, &unbounded);
        let children = self.children_triggers(&start, &triggers, FunctionState::Any, &unbounded);
        triggers.extend(children);
        let dependencies = self.dependency_walk(&start, true, FunctionState::Any, &unbounded);
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
