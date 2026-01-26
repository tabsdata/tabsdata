//
// Copyright 2025 Tabs Data Inc.
//

use td_objects::types::basic::Depth;
use td_objects::types::recursion::{RecursionOptions, RecursionOptionsV1};

pub mod transaction;

pub const EXECUTION_GRAPH_OPTIONS: RecursionOptions = RecursionOptions::V1(RecursionOptionsV1 {
    max_upstream_depth: Some(Depth::ZERO),
    max_downstream_depth: Some(Depth::ZERO),
    override_disabled_trigger: None,
});
