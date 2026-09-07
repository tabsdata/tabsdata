//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

pub(crate) mod logs;

#[cfg(test)]
pub(crate) mod tests {
    use std::env;
    use std::path::PathBuf;
    use td_common::env::WORKSPACE_URI_ENV;
    use td_common::worker::WorkerClass::Ephemeral;
    use td_common::worker::{CAST_FOLDER, LOG_FOLDER, PROC_FOLDER, WORK_FOLDER, WorkerName};
    use td_objects::dxo::worker::{LogsExtension, WorkerDB};
    use td_objects::types::basic::WorkerType;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    pub(crate) fn worker_path(worker_path: &str, worker_db: &WorkerDB, cast: usize) -> PathBuf {
        PathBuf::from(worker_path)
            .join(WORK_FOLDER)
            .join(PROC_FOLDER)
            .join(Ephemeral.as_ref())
            .join(match worker_db.worker_type {
                WorkerType::FunctionRunner => WorkerName::Function.as_ref(),
                WorkerType::CronTrigger => WorkerName::CronTrigger.as_ref(),
                WorkerType::StageTrigger => WorkerName::StageTrigger.as_ref(),
            })
            .join(WORK_FOLDER)
            .join(CAST_FOLDER)
            .join(format!("{}_{}", worker_db.id, &cast.to_string()))
            .join(WORK_FOLDER)
            .join(LOG_FOLDER)
    }

    // Creates logs files for testing. It creates a directory structure similar to the real one,
    // in a testing directory (provided through WORKSPACE_URI_ENV).
    // It creates a worker retry folder for each iteration (_1, _2, etc.).
    // It creates a worker rotation file for each rotation (_1, _2, etc.).
    pub(crate) async fn create_log_files(
        worker_db: &WorkerDB,
        iterations: usize,
        rotations: usize,
    ) {
        for iteration in 1..=iterations {
            let worker_path =
                worker_path(&env::var(WORKSPACE_URI_ENV).unwrap(), worker_db, iteration);
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
