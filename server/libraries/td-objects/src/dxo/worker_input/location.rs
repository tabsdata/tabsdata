//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::EnvPrefix;
use url::Url;

#[ta_objects::Dto]
#[derive(Eq, PartialEq)]
pub struct Location {
    pub uri: Url,
    pub env_prefix: Option<EnvPrefix>,
}

pub trait Locations {
    fn locations(&self) -> Vec<&Location>;
}

impl<T: Locations> Locations for Vec<T> {
    fn locations(&self) -> Vec<&Location> {
        self.iter().flat_map(|t| t.locations()).collect()
    }
}
