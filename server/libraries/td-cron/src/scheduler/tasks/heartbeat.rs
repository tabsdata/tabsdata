//
// Copyright 2025 Tabs Data Inc.
//

use crate::scheduler::runner::{CronJobSchedulerError, CronJobTask};
use async_trait::async_trait;
use tokio::sync::watch;
use tracing::info;

#[derive(Debug, Default)]
pub struct Heartbeat {
    output: String,
}

impl Heartbeat {
    pub fn new(output: String) -> Self {
        Self { output }
    }
}

#[async_trait]
impl CronJobTask for Heartbeat {
    async fn run(&self, _shutdown: watch::Receiver<()>) -> Result<(), CronJobSchedulerError> {
        info!("Heartbeat: {}", self.output);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_heartbeat_run() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(());
        let task = Heartbeat {
            output: "test output".to_string(),
        };
        assert!(task.run(shutdown_rx).await.is_ok());
    }
}
