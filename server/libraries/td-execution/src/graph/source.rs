//
// Copyright 2026 Tabsdata Inc.
//

use crate::graph::FunctionState;
use crate::topology::ProjectTopology;
use ta_execution::GraphSource;
use td_objects::dxo::trigger::TriggerDBWithNames;
use td_objects::types::basic::{FunctionId, FunctionStatus};
use td_objects::types::recursion::RecursionOptions;

/// A topology version seen through the functions a plan may run.
pub struct PlanSource<'t> {
    pub topology: &'t ProjectTopology,
    pub state: FunctionState<'t>,
}

impl GraphSource for PlanSource<'_> {
    /// An overridden start passes while active, even when disabled.
    fn walk(&self, start: &FunctionId, options: &RecursionOptions) -> Vec<&TriggerDBWithNames> {
        let start_passes = options.override_disabled_trigger()
            && self
                .topology
                .functions
                .get(start)
                .is_some_and(|f| f.base.base.status == FunctionStatus::Active);
        self.topology
            .trigger_walk(start, start_passes, self.state, options)
    }

    fn children_triggers<'a>(
        &'a self,
        start: &FunctionId,
        triggers: &[&'a TriggerDBWithNames],
        options: &RecursionOptions,
    ) -> Vec<&'a TriggerDBWithNames> {
        self.topology
            .children_triggers(start, triggers, self.state, options)
    }
}
