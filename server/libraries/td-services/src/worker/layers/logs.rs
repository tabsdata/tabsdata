//
// Copyright 2025 Tabs Data Inc.
//

use bytes::{Bytes, BytesMut};
use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use futures_util::{stream, TryStreamExt};
use glob::glob;
use std::env;
use std::path::PathBuf;
use td_common::server::WorkerClass::EPHEMERAL;
use td_common::server::WorkerName::FUNCTION;
use td_common::server::{CAST_FOLDER, LOG_FOLDER, PROC_FOLDER, WORKSPACE_URI_ENV, WORK_FOLDER};
use td_error::{td_error, TdError};
use td_objects::rest_urls::LogsExtension;
use td_objects::types::basic::{LogsCastNumber, WorkerId};
use td_objects::types::stream::BoxedSyncStream;
use td_tower::extractors::Input;
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
    Input(worker_id): Input<WorkerId>,
    Input(extensions): Input<Vec<LogsExtension>>,
    Input(casts): Input<Vec<LogsCastNumber>>,
) -> Result<Vec<PathBuf>, TdError> {
    let worker_path = env::var(WORKSPACE_URI_ENV).map_err(ReadWorkerLogsError::EnvVar)?;
    let pattern = PathBuf::from(worker_path)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(EPHEMERAL.as_ref())
        .join(FUNCTION.as_ref())
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
                .join(format!("{}_{}", worker_id, cast_pattern))
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
        // Compare parent directories first (so _0, then _1, etc.).
        let a_parent = a.parent().and_then(|p| p.to_str()).unwrap_or("");
        let b_parent = b.parent().and_then(|p| p.to_str()).unwrap_or("");
        let parent_cmp = a_parent.cmp(b_parent);
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

pub async fn get_worker_logs(
    Input(paths): Input<Vec<PathBuf>>,
) -> Result<BoxedSyncStream, TdError> {
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
    use testdir::testdir;

    #[test]
    fn test_sort_log_paths() {
        let mut paths = vec![
            PathBuf::from("/tmp/worker_1/logs/fn_2.log"),
            PathBuf::from("/tmp/worker_0/logs/fn_1.log"),
            PathBuf::from("/tmp/worker_0/logs/fn.log"),
            PathBuf::from("/tmp/worker_1/logs/fn.log"),
            PathBuf::from("/tmp/worker_1/logs/err.log"),
            PathBuf::from("/tmp/worker_0/logs/err.log"),
            PathBuf::from("/tmp/worker_0/logs/err_1.log"),
        ];
        sort_log_paths(&mut paths);
        let expected = vec![
            PathBuf::from("/tmp/worker_0/logs/err.log"),
            PathBuf::from("/tmp/worker_0/logs/err_1.log"),
            PathBuf::from("/tmp/worker_0/logs/fn.log"),
            PathBuf::from("/tmp/worker_0/logs/fn_1.log"),
            PathBuf::from("/tmp/worker_1/logs/err.log"),
            PathBuf::from("/tmp/worker_1/logs/fn.log"),
            PathBuf::from("/tmp/worker_1/logs/fn_2.log"),
        ];
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_sort_log_paths_same_parent() {
        let mut paths = vec![
            PathBuf::from("/a/b/c/fn_1.log"),
            PathBuf::from("/a/b/c/err.log"),
            PathBuf::from("/a/b/c/fn.log"),
            PathBuf::from("/a/b/c/err_2.log"),
        ];
        sort_log_paths(&mut paths);
        let expected = vec![
            PathBuf::from("/a/b/c/err.log"),
            PathBuf::from("/a/b/c/err_2.log"),
            PathBuf::from("/a/b/c/fn.log"),
            PathBuf::from("/a/b/c/fn_1.log"),
        ];
        assert_eq!(paths, expected);
    }

    #[test]
    fn test_sort_log_paths_empty() {
        let mut paths: Vec<PathBuf> = vec![];
        sort_log_paths(&mut paths);
        assert!(paths.is_empty());
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
        let test_dir = testdir!();
        let worker_id = WorkerId::default();

        let (paths, stream) =
            temp_env::async_with_vars([(WORKSPACE_URI_ENV, Some(test_dir.to_str().unwrap()))], {
                let logs_extensions = logs_extensions.clone();
                let casts = casts.clone();
                async move {
                    create_log_files(&worker_id, iterations, rotations).await;
                    let paths = resolve_worker_log_path(
                        Input::new(worker_id),
                        Input::new(logs_extensions),
                        Input::new(casts),
                    )
                    .await?;
                    let stream = get_worker_logs(Input::new(paths.clone())).await?;
                    Ok::<_, TdError>((paths, stream))
                }
            })
            .await?;

        // Assert order of paths (first casts with lower number; then in each cast rotation with lower number).
        assert_eq!(paths, {
            let mut sorted = paths.clone();
            sort_log_paths(&mut sorted);
            sorted
        },);

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
                        &worker_id,
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
