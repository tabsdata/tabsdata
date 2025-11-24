//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::Depth;
use serde::{Deserialize, Serialize};

/// Represents the recursion options used when querying graphs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RecursionOptions {
    V1(RecursionOptionsV1),
}

impl RecursionOptions {
    pub fn new(max_upstream_depth: Option<Depth>, max_downstream_depth: Option<Depth>) -> Self {
        RecursionOptions::V1(RecursionOptionsV1 {
            max_upstream_depth,
            max_downstream_depth,
        })
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
}

#[td_type::Dlo]
#[derive(Copy, Serialize, Deserialize)]
pub struct RecursionOptionsV1 {
    /// Maximum recursive depth when going upstream (toward ancestors)
    pub max_upstream_depth: Option<Depth>,

    /// Maximum recursive depth when going downstream (toward descendants)
    pub max_downstream_depth: Option<Depth>,
}
