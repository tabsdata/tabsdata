//
// Copyright 2026 Tabs Data Inc.
//

use crate::constants::FABRIC_KMINUS;
use crate::topology::Topology;

pub struct KminusTopology;

impl Topology for KminusTopology {
    fn name(&self) -> &'static str {
        FABRIC_KMINUS
    }
}
