//
// Copyright 2026 Tabs Data Inc.
//

use crate::topology::{PROVIDER_PREFIX_K4S, PROVIDER_PREFIX_K8S, ProviderPrefixes, Topology};

pub struct KminusTopology;

const PREFIXES: ProviderPrefixes = ProviderPrefixes {
    keep: PROVIDER_PREFIX_K4S,
    drop: &[PROVIDER_PREFIX_K8S],
};

impl Topology for KminusTopology {
    fn provider_prefixes(&self) -> &'static ProviderPrefixes {
        &PREFIXES
    }
}
