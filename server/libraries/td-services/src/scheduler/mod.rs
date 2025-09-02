//
// Copyright 2025 Tabs Data Inc.
//

use std::fmt::Display;
use std::net::SocketAddr;

mod layers;
pub mod services;

/// Server URL configuration
#[derive(Clone, Debug)]
pub struct ServerUrl(pub SocketAddr);

impl Display for ServerUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "test-utils")]
impl Default for ServerUrl {
    fn default() -> Self {
        ServerUrl(SocketAddr::from(([127, 0, 0, 1], 8080)))
    }
}
