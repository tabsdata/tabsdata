//
// Copyright 2026 Tabs Data Inc.
//

use crate::core::system::exit::CommandResult;
use crate::runtime::provider::Instance;

#[derive(Default)]
pub struct DockerInstance;

impl DockerInstance {
    pub fn new() -> Self {
        Self
    }
}

impl Instance for DockerInstance {
    async fn start(&self) -> CommandResult {
        Ok(())
    }

    async fn stop(&self) -> CommandResult<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        false
    }
}
