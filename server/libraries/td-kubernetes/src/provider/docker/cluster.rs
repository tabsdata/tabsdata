//
// Copyright 2026 Tabs Data Inc.
//

use kube::{Client, Config};

use crate::provider::Cluster;
use crate::system::exit::Result;

pub struct DockerCluster {
    config: Config,
}

impl DockerCluster {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl Cluster for DockerCluster {
    fn create(&self) -> Result {
        Ok(())
    }

    async fn start(&self) -> Result {
        Ok(())
    }

    async fn stop(&self) -> Result<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        let Ok(client) = Client::try_from(self.config.clone()) else {
            return false;
        };
        client.apiserver_version().await.is_ok()
    }
}
