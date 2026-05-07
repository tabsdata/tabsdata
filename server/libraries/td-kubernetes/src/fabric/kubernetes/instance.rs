//
// Copyright 2026 Tabs Data Inc.
//

use async_trait::async_trait;

use crate::fabric::Instance;
use crate::system::exit::Outcome;

#[derive(Default)]
pub struct KubernetesInstance;

impl KubernetesInstance {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Instance for KubernetesInstance {
    async fn start(&self) -> Outcome {
        Ok(())
    }

    async fn stop(&self) -> Outcome<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        false
    }
}
