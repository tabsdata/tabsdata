//
// Copyright 2026 Tabs Data Inc.
//

use async_trait::async_trait;
use kube::{Client, Config};

use crate::fabric::Cluster;
use crate::system::exit::Outcome;

pub struct KubernetesCluster {
    config: Config,
}

impl KubernetesCluster {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Cluster for KubernetesCluster {
    fn create(&self) -> Outcome {
        Ok(())
    }

    async fn start(&self) -> Outcome {
        Ok(())
    }

    async fn stop(&self) -> Outcome<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        let Ok(client) = Client::try_from(self.config.clone()) else {
            return false;
        };
        client.apiserver_version().await.is_ok()
    }
}
