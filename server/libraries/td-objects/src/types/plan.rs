//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::{FunctionOnly, OverrideDisabledTrigger};

/// What a trigger asked of its plan; each edition decides what they mean.
#[derive(Clone, Copy, Debug)]
pub struct PlanOptions {
    pub function_only: FunctionOnly,
    pub override_disabled_trigger: OverrideDisabledTrigger,
}
