//
// Copyright 2026 Tabs Data Inc.
//

use crate::runtime::provider::Instance;
use crate::core::system::exit::Result;

#[derive(Default)]
pub struct DockerInstance;

impl DockerInstance {
    pub fn new() -> Self {
        Self
    }
}

impl Instance for DockerInstance {
    async fn start(&self) -> Result {
        Ok(())
    }

    async fn stop(&self) -> Result<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        false
    }
}
