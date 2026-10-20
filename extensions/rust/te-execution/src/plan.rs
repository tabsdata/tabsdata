//
// Copyright 2026 Tabsdata Inc.
//

use ta_execution::{PlanGraph, PlanScope};
use td_objects::types::basic::FunctionId;
use td_objects::types::plan::PlanOptions;

pub struct ExecutionScope;

impl PlanScope for ExecutionScope {
    /// An execution runs its start function and the start's children.
    fn plan_triggers<'a, G: PlanGraph>(
        graph: &'a G,
        _start: &FunctionId,
        _options: &PlanOptions,
    ) -> Vec<&'a G::Trigger> {
        graph.children_triggers(&[])
    }
}
