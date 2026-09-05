//
// Copyright 2026 Tabsdata Inc.
//

use kube::{Client, Config};

use crate::core::system::exit::CommandResult;
use crate::runtime::provider::Cluster;

pub struct CloudCluster {
    config: Config,
}

impl CloudCluster {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl Cluster for CloudCluster {
    fn create(&self) -> CommandResult {
        Ok(())
    }

    async fn start(&self) -> CommandResult {
        Ok(())
    }

    async fn stop(&self) -> CommandResult<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        let Ok(client) = Client::try_from(self.config.clone()) else {
            return false;
        };
        client.apiserver_version().await.is_ok()
    }
}
