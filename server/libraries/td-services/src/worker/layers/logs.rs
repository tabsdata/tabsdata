//
// Copyright 2025 Tabs Data Inc.
//

use bytes::{Bytes, BytesMut};
use futures_util::stream::FuturesOrdered;
use futures_util::{StreamExt, TryStreamExt, stream};
use glob::glob;
use std::env;
use std::path::PathBuf;
use td_common::server::WorkerClass::Ephemeral;
use td_common::server::{
    CAST_FOLDER, LOG_FOLDER, PROC_FOLDER, WORK_FOLDER, WORKSPACE_URI_ENV, WorkerName,
};
use td_error::{TdError, td_error};
use td_objects::dxo::worker::WorkerDB;
use td_objects::rest_urls::LogsExtension;
use td_objects::types::basic::{LogsCastNumber, WorkerType};
use td_objects::types::stream::BoxedSyncStream;
use td_tower::extractors::Ref;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

#[td_error]
enum ReadWorkerLogsError {
    #[error("Failed to resolve worker log path")]
    EnvVar(#[from] env::VarError) = 5001,
    #[error("Glob error trying to resolve log path")]
    Glob(#[from] glob::GlobError) = 5002,
    #[error("Pattern error trying to resolve log path")]
    Pattern(#[from] glob::PatternError) = 5003,
    #[error("Error trying to build Glob pattern to resolve log path")]
    EmptyPattern = 5004,
    #[error("Failed to read worker log")]
    IoError(#[from] std::io::Error) = 5005,
}

const SEPARATOR_LEN: usize = 50;
const PATH_SEPARATOR: &str = "-";
const LOG_SEPARATOR: &str = "=";

pub async fn resolve_worker_log_path(
    worker: Ref<WorkerDB>,
    extensions: Ref<Vec<LogsExtension>>,
    casts: Ref<Vec<LogsCastNumber>>,
) -> Result<Vec<PathBuf>, TdError> {
    let worker_path = env::var(WORKSPACE_URI_ENV).map_err(ReadWorkerLogsError::EnvVar)?;
    let worker_name = match worker.worker_type {
        WorkerType::FunctionRunner => WorkerName::Function,
        WorkerType::CronTrigger => WorkerName::CronTrigger,
        WorkerType::StageTrigger => WorkerName::StageTrigger,
    };
    let pattern = PathBuf::from(worker_path)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(Ephemeral.as_ref())
        .join(worker_name.as_ref())
        .join(WORK_FOLDER)
        .join(CAST_FOLDER);

    let casts_glob_patterns = if casts.is_empty() {
        vec!["*".to_string()]
    } else {
        casts.iter().map(|c| c.to_string()).collect::<Vec<_>>()
    };

    let mut paths = Vec::new();
    for cast_pattern in casts_glob_patterns.iter() {
        for extension in extensions.iter() {
            let pattern = pattern
                .clone()
                .join(format!("{}_{}", worker.id, cast_pattern))
                .join(WORK_FOLDER)
                .join(LOG_FOLDER)
                .join(extension.glob_pattern());
            let pattern = pattern.to_str().ok_or(ReadWorkerLogsError::EmptyPattern)?;

            for entry in glob(pattern).map_err(ReadWorkerLogsError::Pattern)? {
                match entry {
                    Ok(path) => paths.push(path),
                    Err(e) => Err(ReadWorkerLogsError::Glob(e))?,
                }
            }
        }
    }
    sort_log_paths(&mut paths);

    Ok(paths)
}

fn sort_log_paths(paths: &mut [PathBuf]) {
    paths.sort_by(|a, b| {
        // Compare parent directories first by modified time.
        let a_parent = a
            .parent()
            .and_then(|p| std::fs::metadata(p).ok())
            .and_then(|m| m.modified().ok());
        let b_parent = b
            .parent()
            .and_then(|p| std::fs::metadata(p).ok())
            .and_then(|m| m.modified().ok());

        let parent_cmp = match (a_parent, b_parent) {
            (Some(a_parent), Some(b_parent)) => a_parent.cmp(&b_parent).reverse(),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if parent_cmp != std::cmp::Ordering::Equal {
            return parent_cmp;
        }

        // Then compare file names (when logs rotate: err.log, fn.log, fn_1.log ...).
        let parse = |path: &PathBuf| {
            let base = path.file_stem().and_then(|n| n.to_str()).unwrap_or("");
            let (base, num) = base.split_once('_').map_or((base, "0"), |(b, n)| (b, n));
            let num = num.parse::<u32>().unwrap_or(0);
            (base.to_string(), num)
        };

        let (a_base, a_num) = parse(a);
        let (b_base, b_num) = parse(b);

        a_base.cmp(&b_base).then(a_num.cmp(&b_num))
    });
}

pub async fn get_worker_logs(paths: Ref<Vec<PathBuf>>) -> Result<BoxedSyncStream, TdError> {
    let mut ordered_stream = FuturesOrdered::new();

    for path in paths.iter() {
        let path = path.clone();

        // Build the stream for the current path.
        let stream = async move {
            // Open the file and create a stream for its contents
            let file = File::open(&path)
                .await
                .map_err(ReadWorkerLogsError::IoError)?;
            let file_stream = FramedRead::new(file, BytesCodec::new())
                .map_ok(BytesMut::freeze)
                .map_err(ReadWorkerLogsError::IoError);

            // Stream the path as the first item
            let path_stream = stream::once(async move {
                let path_bytes = Bytes::from(format!("{}\n", path.display()));
                Ok(path_bytes)
            });

            // Separator stream
            let path_separator_stream = stream::once(async move {
                let path_bytes = Bytes::from(format!("{}\n", PATH_SEPARATOR.repeat(SEPARATOR_LEN)));
                Ok(path_bytes)
            });

            // Separator stream
            let log_separator_stream = stream::once(async move {
                let path_bytes = Bytes::from(format!("{}\n", LOG_SEPARATOR.repeat(SEPARATOR_LEN)));
                Ok(path_bytes)
            });

            // Combine the path stream and file stream
            let combined_stream = path_stream
                .chain(path_separator_stream)
                .chain(file_stream)
                .chain(log_separator_stream)
                .map_err(TdError::from);
            Ok::<_, TdError>(combined_stream)
        };

        // Push the stream to the ordered stream futures.
        ordered_stream.push_back(Box::pin(stream));
    }

    // Flattening the ordered stream preserves the order of the streams.
    let aggregated_stream = ordered_stream.try_flatten();
    Ok(BoxedSyncStream::new(aggregated_stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::layers::tests::{create_log_files, worker_path};
    use strum::IntoEnumIterator;

    use td_objects::types::basic::FunctionVersionId;
    use td_objects::types::basic::{Enabled, WorkerMessageStatus, WorkerStatus};

    #[test]
    fn test_sort_logs_by_filename_and_rotation() {
        let mut paths = vec![
            PathBuf::from("err_1.log"),
            PathBuf::from("app.log"),
            PathBuf::from("err.log"),
            PathBuf::from("fn_2.log"),
            PathBuf::from("fn.log"),
            PathBuf::from("fn_1.log"),
        ];

        sort_log_paths(&mut paths);

        assert_eq!(paths[0], PathBuf::from("app.log"));
        assert_eq!(paths[1], PathBuf::from("err.log"));
        assert_eq!(paths[2], PathBuf::from("err_1.log"));
        assert_eq!(paths[3], PathBuf::from("fn.log"));
        assert_eq!(paths[4], PathBuf::from("fn_1.log"));
        assert_eq!(paths[5], PathBuf::from("fn_2.log"));
    }

    #[test]
    fn test_sort_logs_by_parent_modified_time() {
        let temp = td_common::tdtestdir!();

        let dir1 = temp.join("dir1");
        let dir2 = temp.join("dir2");
        std::fs::create_dir(&dir1).unwrap();
        std::fs::create_dir(&dir2).unwrap();

        let file1 = dir1.join("app.log");
        let file2 = dir2.join("app.log");
        std::fs::write(&file1, "").unwrap();
        std::fs::write(&file2, "").unwrap();

        // Set explicit timestamps to ensure dir2 is newer
        let old_time = filetime::FileTime::from_unix_time(1000, 0);
        let new_time = filetime::FileTime::from_unix_time(2000, 0);

        filetime::set_file_mtime(&dir1, old_time).unwrap();
        filetime::set_file_mtime(&dir2, new_time).unwrap();

        let mut paths = vec![file1.clone(), file2.clone()];
        sort_log_paths(&mut paths);

        // dir2 was modified more recently, so it should come first (reverse order)
        assert_eq!(paths[0], file2);
        assert_eq!(paths[1], file1);
    }

    #[test]
    fn test_sort_logs_empty_and_edge_cases() {
        // Empty
        let mut paths: Vec<PathBuf> = vec![];
        sort_log_paths(&mut paths);
        assert!(paths.is_empty());

        // Single path
        let mut paths = vec![PathBuf::from("single.log")];
        sort_log_paths(&mut paths);
        assert_eq!(paths[0], PathBuf::from("single.log"));
    }

    #[test]
    fn test_sort_logs_complex_scenario() {
        let temp = td_common::tdtestdir!();

        let old_dir = temp.join("old");
        let new_dir = temp.join("new");
        std::fs::create_dir(&old_dir).unwrap();
        std::fs::create_dir(&new_dir).unwrap();

        let mut paths = vec![
            old_dir.join("app_2.log"),
            new_dir.join("app.log"),
            old_dir.join("app.log"),
            new_dir.join("err.log"),
            old_dir.join("app_1.log"),
        ];

        for path in &paths {
            std::fs::write(path, "").unwrap();
        }

        // Set explicit timestamps to ensure new_dir is newer
        let old_time = filetime::FileTime::from_unix_time(1000, 0);
        let new_time = filetime::FileTime::from_unix_time(2000, 0);

        filetime::set_file_mtime(&old_dir, old_time).unwrap();
        filetime::set_file_mtime(&new_dir, new_time).unwrap();

        sort_log_paths(&mut paths);

        // New dir comes first (most recent), then within each dir, sort by name and rotation
        assert_eq!(paths[0], new_dir.join("app.log"));
        assert_eq!(paths[1], new_dir.join("err.log"));
        assert_eq!(paths[2], old_dir.join("app.log"));
        assert_eq!(paths[3], old_dir.join("app_1.log"));
        assert_eq!(paths[4], old_dir.join("app_2.log"));
    }

    // Tests the resolve_worker_log_path and get_worker_logs functions. It creates a test directory,
    // sets the WORKSPACE_URI_ENV variable to it, and creates log files for each extension.
    // The logs read depend on and the extension chosen.
    // CARE: Using temp_env::async_with_vars is thread safe as long as all tests use it. If other
    // tests are run in parallel that do not use this, it may cause issues.
    async fn test_worker_logs(
        logs_extensions: Vec<LogsExtension>, // log extensions to look for
        casts: Vec<LogsCastNumber>,          // cast numbers to look for
        iterations: usize,                   // number of casts created (per worker)
        rotations: usize,                    // number of logs created (per cast)
    ) -> Result<(Vec<PathBuf>, String), TdError> {
        let test_dir = td_common::tdtestdir!();
        let worker_db = WorkerDB {
            id: Default::default(),
            collection_id: Default::default(),
            execution_id: None,
            transaction_id: None,
            function_run_id: None,
            function_version_id: FunctionVersionId::new(),
            message_status: WorkerMessageStatus::Unlocked,
            started_on: None,
            ended_on: None,
            status: WorkerStatus::Scheduled,
            worker_type: WorkerType::FunctionRunner,
            worker_enabled: Enabled::TRUE,
            worker_config: None,
        };

        let worker = worker_db.clone();
        let (paths, stream) =
            temp_env::async_with_vars([(WORKSPACE_URI_ENV, Some(test_dir.to_str().unwrap()))], {
                let logs_extensions = logs_extensions.clone();
                let casts = casts.clone();
                async move {
                    create_log_files(&worker, iterations, rotations).await;
                    let paths = resolve_worker_log_path(
                        Ref::new(worker),
                        Ref::new(logs_extensions),
                        Ref::new(casts),
                    )
                    .await?;
                    let stream = get_worker_logs(Ref::new(paths.clone())).await?;
                    Ok::<_, TdError>((paths, stream))
                }
            })
            .await?;

        // Assert order of paths (first casts with lower number; then in each cast rotation with lower number).
        assert_eq!(paths, {
            let mut sorted = paths.clone();
            sort_log_paths(&mut sorted);
            sorted
        });

        // Get file content (there is only one file with all the logs).
        let content = stream.into_inner().try_collect::<Vec<Bytes>>().await?;
        let content = content
            .iter()
            .flat_map(|b| b.iter())
            .cloned()
            .collect::<Vec<_>>();
        let content = String::from_utf8_lossy(&content);

        // One log file per extension per rotation, in each iteration, in each format.
        let expected_log_files_per_cast = logs_extensions
            .iter()
            .flat_map(|f| f.files(rotations))
            .collect::<Vec<_>>();

        // Get files in each cast (retry of the worker).
        let casts_found = if casts.is_empty() {
            // If casts are not specified, we assume all iterations (as the test should look for every cast).
            (1..=iterations)
                .map(|i| LogsCastNumber::try_from(i as i16).unwrap())
                .collect::<Vec<_>>()
        } else {
            casts
        };
        let mut all_paths = paths.clone();
        for cast in casts_found.iter() {
            let mut cast_paths = vec![];
            all_paths.retain(|log_file| {
                let keep = log_file.parent()
                    != Some(&*worker_path(
                        &test_dir.to_string_lossy(),
                        &worker_db,
                        **cast as usize,
                    ));
                if !keep {
                    cast_paths.push(log_file.clone());
                }
                keep
            });

            // Assert files and content are correct.
            assert_eq!(cast_paths.len(), expected_log_files_per_cast.len());
            for log_file in &expected_log_files_per_cast {
                let path = cast_paths
                    .iter()
                    .find(|p| p.file_name() == Some(log_file.as_ref()));
                assert!(
                    path.is_some(),
                    "Log file for [{}] not found",
                    log_file.to_string_lossy()
                );
                // Given that log filers are created with the content being the file name.
                assert!(
                    content.contains(log_file.to_string_lossy().to_string().as_str()),
                    "Content does not contain log file [{}]",
                    log_file.to_string_lossy()
                );
            }
        }
        // Make sure we went through all paths.
        assert!(all_paths.is_empty());

        Ok((paths, content.to_string()))
    }

    #[tokio::test]
    async fn test_resolve_worker_logs() -> Result<(), TdError> {
        for extension in LogsExtension::iter() {
            test_worker_logs(vec![extension], vec![], 1, 1).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_worker_logs_multiple() -> Result<(), TdError> {
        test_worker_logs(vec![LogsExtension::Fn, LogsExtension::Out], vec![], 1, 1).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_worker_logs_single_cast() -> Result<(), TdError> {
        for extension in LogsExtension::iter() {
            test_worker_logs(vec![extension], vec![LogsCastNumber::try_from(1)?], 1, 1).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_worker_logs_retries() -> Result<(), TdError> {
        for extension in LogsExtension::iter() {
            test_worker_logs(vec![extension], vec![], 3, 1).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_worker_logs_rotation() -> Result<(), TdError> {
        let _ = test_worker_logs(vec![LogsExtension::All], vec![], 1, 2).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_worker_logs_retry_and_rotation() -> Result<(), TdError> {
        let _ = test_worker_logs(vec![LogsExtension::All], vec![], 2, 2).await?;
        Ok(())
    }
}
