//
// Copyright 2026 Tabs Data Inc.
//

use crate::fabric::Instance;
use crate::system::exit::Result;

#[derive(Default)]
pub struct KubernetesInstance;

impl KubernetesInstance {
    pub fn new() -> Self {
        Self
    }
}

impl Instance for KubernetesInstance {
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
