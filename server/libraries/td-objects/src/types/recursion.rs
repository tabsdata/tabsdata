//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{Depth, OverrideDisabledTrigger};
use serde::{Deserialize, Serialize};

/// Represents the recursion options used when querying graphs.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum RecursionOptions {
    V1(RecursionOptionsV1),
}

impl RecursionOptions {
    pub fn new(max_upstream_depth: Option<Depth>, max_downstream_depth: Option<Depth>) -> Self {
        RecursionOptions::V1(RecursionOptionsV1 {
            max_upstream_depth,
            max_downstream_depth,
            override_disabled_trigger: None,
        })
    }

    pub fn with_override_disabled_trigger(
        &mut self,
        override_disabled_trigger: OverrideDisabledTrigger,
    ) -> &mut Self {
        match self {
            RecursionOptions::V1(options) => {
                options.override_disabled_trigger = Some(override_disabled_trigger);
            }
        }
        self
    }

    pub fn max_upstream_depth(&self) -> Option<Depth> {
        match self {
            RecursionOptions::V1(options) => options.max_upstream_depth,
        }
    }

    pub fn max_downstream_depth(&self) -> Option<Depth> {
        match self {
            RecursionOptions::V1(options) => options.max_downstream_depth,
        }
    }

    pub fn override_disabled_trigger(&self) -> bool {
        match self {
            RecursionOptions::V1(options) => *options
                .override_disabled_trigger
                .unwrap_or(OverrideDisabledTrigger::FALSE),
        }
    }
}

#[ta_objects::Dlo]
#[derive(Copy, Serialize, Deserialize)]
pub struct RecursionOptionsV1 {
    /// Maximum recursive depth when going upstream (toward ancestors)
    pub max_upstream_depth: Option<Depth>,

    /// Maximum recursive depth when going downstream (toward descendants)
    pub max_downstream_depth: Option<Depth>,

    /// Flag to allow disabled main trigger.
    #[serde(default)]
    pub override_disabled_trigger: Option<OverrideDisabledTrigger>,
}
