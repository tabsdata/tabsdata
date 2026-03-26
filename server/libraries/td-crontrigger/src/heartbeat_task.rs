//
// Copyright 2026. Tabs Data Inc.
//

//! A task for touching a file, typically used for heartbeats.
//
// Copyright 2026. Tabs Data Inc.
//

use crate::task_runner::Task;
use async_trait::async_trait;
use std::fmt;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use td_error::td_error;
use tokio::sync::watch;
use tracing::debug;

/// A task that "touches" a file, creating it if it doesn't exist
/// or updating its modification time if it does. This is useful for
/// signaling liveness.
#[derive(Debug)]
pub struct TouchFileTask {
    path: PathBuf,
}

impl TouchFileTask {
    /// Creates a new `TouchFileTask`.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let task = Self {
            path: path.as_ref().to_path_buf(),
        };
        debug!("Creating, {task}.");
        task
    }
}

/// Errors that can occur within the `TouchFileTask`.
#[td_error]
pub enum TouchFileTaskError {
    /// An I/O error occurred while trying to touch the file.
    #[error("IO error for file '{0}': {1}")]
    IoError(PathBuf, #[source] std::io::Error),
}

#[async_trait]
impl Task for TouchFileTask {
    type Error = TouchFileTaskError;

    async fn run(&self, _runner_shutdown_rx: watch::Receiver<()>) -> Result<(), Self::Error> {
        debug!("Touch, {self}.");

        // Open the file.
        // - `create(true)`: Creates the file if it doesn't exist.
        // - `truncate(false)`: Don't truncate the file.
        // - `write(true)`: Required to update metadata/timestamps.
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.path)
            .map_err(|e| TouchFileTaskError::IoError(self.path.clone(), e))?;

        // Explicitly update the modification time to "now".
        file.set_modified(SystemTime::now())
            .map_err(|e| TouchFileTaskError::IoError(self.path.clone(), e))?;

        Ok(())
    }
}

impl Display for TouchFileTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "file: {}", self.path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_touch_file_task() {
        let testdir = td_common::tdtestdir!();
        let file_path = testdir.join("heartbeat.txt");
        let task = TouchFileTask::new(&file_path);
        let (_shutdown_tx, shutdown_rx) = watch::channel(());

        // 1. Test file creation
        assert!(!file_path.exists(), "File should not exist initially");
        task.run(shutdown_rx.clone()).await.unwrap();
        assert!(file_path.exists(), "File should be created after first run");

        // 2. Test file modification time update
        let initial_metadata = fs::metadata(&file_path).unwrap();
        let initial_mod_time = initial_metadata.modified().unwrap();

        // Sleep for a bit to ensure the modification time will be different
        sleep(Duration::from_millis(10)).await;

        task.run(shutdown_rx).await.unwrap();

        let updated_metadata = fs::metadata(&file_path).unwrap();
        let updated_mod_time = updated_metadata.modified().unwrap();

        assert!(
            updated_mod_time > initial_mod_time,
            "Modification time should be updated after second run"
        );
    }
}
