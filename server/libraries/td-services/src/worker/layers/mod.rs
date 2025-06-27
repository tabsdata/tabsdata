//
// Copyright 2025 Tabs Data Inc.
//

pub(crate) mod logs;

#[cfg(test)]
pub(crate) mod tests {
    use std::env;
    use std::path::PathBuf;
    use td_common::server::WorkerClass::EPHEMERAL;
    use td_common::server::WorkerName::FUNCTION;
    use td_common::server::{CAST_FOLDER, LOG_FOLDER, PROC_FOLDER, WORKSPACE_URI_ENV, WORK_FOLDER};
    use td_objects::rest_urls::LogsExtension;
    use td_objects::types::basic::WorkerId;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    // Constructs the path to the worker logs directory based on the worker ID and iteration.
    pub(crate) fn worker_path(worker_path: &str, worker_id: &WorkerId, cast: usize) -> PathBuf {
        PathBuf::from(worker_path)
            .join(WORK_FOLDER)
            .join(PROC_FOLDER)
            .join(EPHEMERAL.as_ref())
            .join(FUNCTION.as_ref())
            .join(WORK_FOLDER)
            .join(CAST_FOLDER)
            .join(format!("{}_{}", worker_id, &cast.to_string()))
            .join(WORK_FOLDER)
            .join(LOG_FOLDER)
    }

    // Creates logs files for testing. It creates a directory structure similar to the real one,
    // in a testing directory (provided through WORKSPACE_URI_ENV).
    // It creates a worker retry folder for each iteration (_1, _2, etc.).
    // It creates a worker rotation file for each rotation (_1, _2, etc.).
    pub(crate) async fn create_log_files(
        worker_id: &WorkerId,
        iterations: usize,
        rotations: usize,
    ) {
        for iteration in 1..=iterations {
            let worker_path =
                worker_path(&env::var(WORKSPACE_URI_ENV).unwrap(), worker_id, iteration);
            tokio::fs::create_dir_all(&worker_path).await.unwrap();

            for log_file in LogsExtension::All.files(rotations) {
                let log_path = worker_path.join(&log_file);
                let mut file_out = File::create_new(&log_path).await.unwrap();
                let content = format!("{}", log_file.to_string_lossy());
                file_out.write_all(content.as_bytes()).await.unwrap();
            }
        }
    }
}
