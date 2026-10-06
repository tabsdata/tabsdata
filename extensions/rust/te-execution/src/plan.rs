//
// Copyright 2026 Tabsdata Inc.
//

use ta_execution::{GraphSource, PlanScope};
use td_error::TdError;
use td_objects::dxo::trigger::TriggerDBWithNames;
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

    async fn plan_triggers(
        source: &mut impl GraphSource,
        start: &FunctionId,
        options: &RecursionOptions,
    ) -> Result<Vec<TriggerDBWithNames>, TdError> {
        source.walk(start, options).await
    }
}
