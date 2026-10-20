//
// Copyright 2026 Tabsdata Inc.
//

use ta_execution::{PlanScope, TriggerSource};
use td_objects::types::basic::{Depth, FunctionId, FunctionOnly, OverrideDisabledTrigger};
use td_objects::types::recursion::RecursionOptions;

pub struct ExecutionScope;

impl PlanScope for ExecutionScope {
    fn execution_recursion_options(
        _function_only: FunctionOnly,
        override_disabled_trigger: OverrideDisabledTrigger,
    ) -> RecursionOptions {
        let mut options = RecursionOptions::new(Some(Depth::ZERO), Some(Depth::ZERO));
        options.with_override_disabled_trigger(override_disabled_trigger);
        options
    }

    /// An execution runs its start function and the start's children.
    fn plan_triggers<'a, S: TriggerSource>(
        source: &'a S,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> Vec<&'a S::Trigger> {
        source.children_triggers(start, &[], options)
    }
}
