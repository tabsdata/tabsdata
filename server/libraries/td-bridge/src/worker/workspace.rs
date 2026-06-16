//
// Copyright 2026 Tabs Data Inc.
//

//! The local workspace worker handler: reads the log files a worker leaves
//! under the workspace's `proc/` tree.

use super::{LogBridgeError, WorkerHandler, WorkerLogStream, WorkerLogTarget};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::path::PathBuf;
use td_common::env::WORKSPACE_URI_ENV;
use td_common::worker::{CAST_FOLDER, EPHEMERAL_FOLDER, LOG_FOLDER, PROC_FOLDER, WORK_FOLDER};

/// Reads the log files a worker leaves in the local workspace.
pub struct WorkspaceWorkerHandler;

#[async_trait]
impl WorkerHandler for WorkspaceWorkerHandler {
    async fn logs(&self, target: &WorkerLogTarget) -> Result<WorkerLogStream, LogBridgeError> {
        let paths = resolve_worker_log_files(target)?;
        Ok(stream_worker_log_files(paths))
    }

    async fn logs_tail(
        &self,
        _target: &WorkerLogTarget,
    ) -> Result<WorkerLogStream, LogBridgeError> {
        Err(LogBridgeError::TailUnsupported)
    }
}

const LOG_PATH_SEPARATOR: &str = "-";
const LOG_CONTENT_SEPARATOR: &str = "=";
const LOG_SEPARATOR_LEN: usize = 50;
const LOG_READ_BUFFER_LEN: usize = 64 * 1024;

fn resolve_worker_log_files(target: &WorkerLogTarget) -> Result<Vec<PathBuf>, LogBridgeError> {
    let workspace =
        std::env::var(WORKSPACE_URI_ENV).map_err(|error| LogBridgeError::WorkspaceLocation {
            cause: error.to_string(),
        })?;
    let base = PathBuf::from(workspace)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(EPHEMERAL_FOLDER)
        .join(target.worker_kind.as_ref())
        .join(WORK_FOLDER)
        .join(CAST_FOLDER);

    let casts = if target.casts.is_empty() {
        vec!["*".to_string()]
    } else {
        target.casts.clone()
    };

    let mut paths = Vec::new();
    for cast in casts.iter() {
        for log_pattern in target.log_patterns.iter() {
            let pattern = base
                .clone()
                .join(format!("{}_{}", target.worker_id, cast))
                .join(WORK_FOLDER)
                .join(LOG_FOLDER)
                .join(log_pattern);
            let pattern = pattern.to_str().ok_or(LogBridgeError::EmptyPattern)?;
            for entry in glob::glob(pattern).map_err(|error| LogBridgeError::Glob {
                cause: error.to_string(),
            })? {
                let path = entry.map_err(|error| LogBridgeError::Glob {
                    cause: error.to_string(),
                })?;
                paths.push(path);
            }
        }
    }
    sort_worker_log_files(&mut paths);
    Ok(paths)
}

fn sort_worker_log_files(paths: &mut [PathBuf]) {
    paths.sort_by(|a, b| {
        let modified = |path: &PathBuf| {
            path.parent()
                .and_then(|parent| std::fs::metadata(parent).ok())
                .and_then(|metadata| metadata.modified().ok())
        };
        let parent_cmp = match (modified(a), modified(b)) {
            (Some(left), Some(right)) => left.cmp(&right).reverse(),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if parent_cmp != std::cmp::Ordering::Equal {
            return parent_cmp;
        }
        let parse = |path: &PathBuf| {
            let stem = path
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let (base, rotation) = stem.split_once('_').map_or((stem, "0"), |(b, n)| (b, n));
            (base.to_string(), rotation.parse::<u32>().unwrap_or(0))
        };
        let (a_base, a_rotation) = parse(a);
        let (b_base, b_rotation) = parse(b);
        a_base.cmp(&b_base).then(a_rotation.cmp(&b_rotation))
    });
}

enum LogReadState {
    Header(PathBuf),
    Open(PathBuf),
    Read(tokio::fs::File),
    Footer,
    Done,
}

fn read_file_stream(path: PathBuf) -> impl Stream<Item = Result<Bytes, LogBridgeError>> {
    use tokio::io::AsyncReadExt;

    futures::stream::unfold(LogReadState::Header(path), |state| async move {
        match state {
            LogReadState::Header(path) => {
                let header = Bytes::from(format!(
                    "{}\n{}\n",
                    path.display(),
                    LOG_PATH_SEPARATOR.repeat(LOG_SEPARATOR_LEN)
                ));
                Some((Ok(header), LogReadState::Open(path)))
            }
            LogReadState::Open(path) => match tokio::fs::File::open(&path).await {
                Ok(file) => {
                    let mut buffer = vec![0u8; LOG_READ_BUFFER_LEN];
                    let mut file = file;
                    match file.read(&mut buffer).await {
                        Ok(0) => Some((Ok(Bytes::new()), LogReadState::Footer)),
                        Ok(read) => {
                            buffer.truncate(read);
                            Some((Ok(Bytes::from(buffer)), LogReadState::Read(file)))
                        }
                        Err(error) => Some((
                            Err(LogBridgeError::Read {
                                cause: error.to_string(),
                            }),
                            LogReadState::Footer,
                        )),
                    }
                }
                Err(error) => Some((
                    Err(LogBridgeError::Read {
                        cause: error.to_string(),
                    }),
                    LogReadState::Footer,
                )),
            },
            LogReadState::Read(mut file) => {
                let mut buffer = vec![0u8; LOG_READ_BUFFER_LEN];
                match file.read(&mut buffer).await {
                    Ok(0) => Some((Ok(Bytes::new()), LogReadState::Footer)),
                    Ok(read) => {
                        buffer.truncate(read);
                        Some((Ok(Bytes::from(buffer)), LogReadState::Read(file)))
                    }
                    Err(error) => Some((
                        Err(LogBridgeError::Read {
                            cause: error.to_string(),
                        }),
                        LogReadState::Footer,
                    )),
                }
            }
            LogReadState::Footer => {
                let footer = Bytes::from(format!(
                    "{}\n",
                    LOG_CONTENT_SEPARATOR.repeat(LOG_SEPARATOR_LEN)
                ));
                Some((Ok(footer), LogReadState::Done))
            }
            LogReadState::Done => None,
        }
    })
}

fn stream_worker_log_files(paths: Vec<PathBuf>) -> WorkerLogStream {
    use futures::StreamExt;
    Box::pin(
        futures::stream::iter(paths)
            .flat_map(read_file_stream)
            .fuse(),
    )
}

#[cfg(test)]
mod tests_worker_log_files {
    use super::*;
    use futures::TryStreamExt;
    use std::path::Path;
    use td_common::logging::LOG_EXTENSION;
    use td_common::tdtestdir;
    use td_common::worker::WorkerName;

    fn logs_folder(workspace: &Path, kind: WorkerName, worker_id: &str, cast: usize) -> PathBuf {
        PathBuf::from(workspace)
            .join(WORK_FOLDER)
            .join(PROC_FOLDER)
            .join(EPHEMERAL_FOLDER)
            .join(kind.as_ref())
            .join(WORK_FOLDER)
            .join(CAST_FOLDER)
            .join(format!("{worker_id}_{cast}"))
            .join(WORK_FOLDER)
            .join(LOG_FOLDER)
    }

    #[test]
    fn test_sort_worker_log_files_by_filename_and_rotation() {
        let mut paths = vec![
            PathBuf::from("err_1.log"),
            PathBuf::from("app.log"),
            PathBuf::from("err.log"),
            PathBuf::from("fn_2.log"),
            PathBuf::from("fn.log"),
            PathBuf::from("fn_1.log"),
        ];
        sort_worker_log_files(&mut paths);
        assert_eq!(paths[0], PathBuf::from("app.log"));
        assert_eq!(paths[1], PathBuf::from("err.log"));
        assert_eq!(paths[2], PathBuf::from("err_1.log"));
        assert_eq!(paths[3], PathBuf::from("fn.log"));
        assert_eq!(paths[4], PathBuf::from("fn_1.log"));
        assert_eq!(paths[5], PathBuf::from("fn_2.log"));
    }

    #[test]
    fn test_sort_worker_log_files_by_parent_modified_time() {
        let temp = tdtestdir!();
        let dir1 = temp.join("dir1");
        let dir2 = temp.join("dir2");
        std::fs::create_dir(&dir1).unwrap();
        std::fs::create_dir(&dir2).unwrap();
        let file1 = dir1.join("app.log");
        let file2 = dir2.join("app.log");
        std::fs::write(&file1, "").unwrap();
        std::fs::write(&file2, "").unwrap();
        let old_time = filetime::FileTime::from_unix_time(1000, 0);
        let new_time = filetime::FileTime::from_unix_time(2000, 0);
        filetime::set_file_mtime(&dir1, old_time).unwrap();
        filetime::set_file_mtime(&dir2, new_time).unwrap();
        let mut paths = vec![file1.clone(), file2.clone()];
        sort_worker_log_files(&mut paths);
        assert_eq!(paths[0], file2);
        assert_eq!(paths[1], file1);
    }

    #[test]
    fn test_sort_worker_log_files_edge_cases() {
        let mut paths: Vec<PathBuf> = vec![];
        sort_worker_log_files(&mut paths);
        assert!(paths.is_empty());
        let mut paths = vec![PathBuf::from("single.log")];
        sort_worker_log_files(&mut paths);
        assert_eq!(paths[0], PathBuf::from("single.log"));
    }

    #[test]
    fn test_sort_worker_log_files_complex() {
        let temp = tdtestdir!();
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
        let old_time = filetime::FileTime::from_unix_time(1000, 0);
        let new_time = filetime::FileTime::from_unix_time(2000, 0);
        filetime::set_file_mtime(&old_dir, old_time).unwrap();
        filetime::set_file_mtime(&new_dir, new_time).unwrap();
        sort_worker_log_files(&mut paths);
        assert_eq!(paths[0], new_dir.join("app.log"));
        assert_eq!(paths[1], new_dir.join("err.log"));
        assert_eq!(paths[2], old_dir.join("app.log"));
        assert_eq!(paths[3], old_dir.join("app_1.log"));
        assert_eq!(paths[4], old_dir.join("app_2.log"));
    }

    #[tokio::test]
    async fn test_resolve_and_stream_worker_log_files() {
        let temp = tdtestdir!();
        let workspace = temp.to_string_lossy().to_string();
        let worker_id = "worker0001";
        let folder = logs_folder(&temp, WorkerName::Function, worker_id, 1);
        std::fs::create_dir_all(&folder).unwrap();
        for name in ["err.log", "fn.log", "out.log", "td.log"] {
            std::fs::write(folder.join(name), name).unwrap();
        }

        let target = WorkerLogTarget {
            worker_id: worker_id.to_string(),
            worker_kind: WorkerName::Function,
            log_patterns: vec![format!("*.{LOG_EXTENSION}")],
            casts: vec![],
        };

        let (paths, content): (Vec<PathBuf>, String) =
            temp_env::async_with_vars([(WORKSPACE_URI_ENV, Some(workspace.as_str()))], async {
                let paths = resolve_worker_log_files(&target).unwrap();
                let chunks: Vec<Bytes> = stream_worker_log_files(paths.clone())
                    .try_collect()
                    .await
                    .unwrap();
                let content = chunks
                    .iter()
                    .flat_map(|chunk| chunk.iter())
                    .copied()
                    .collect::<Vec<_>>();
                (paths, String::from_utf8_lossy(&content).into_owned())
            })
            .await;

        assert_eq!(paths.len(), 4);
        for name in ["err.log", "fn.log", "out.log", "td.log"] {
            assert!(content.contains(name), "missing log content for {name}");
        }
    }

    #[tokio::test]
    async fn test_resolve_worker_log_files_single_cast_filter() {
        let temp = tdtestdir!();
        let workspace = temp.to_string_lossy().to_string();
        let worker_id = "worker0002";
        for cast in [1usize, 2usize] {
            let folder = logs_folder(&temp, WorkerName::Function, worker_id, cast);
            std::fs::create_dir_all(&folder).unwrap();
            std::fs::write(folder.join("fn.log"), format!("cast {cast}")).unwrap();
        }

        let target = WorkerLogTarget {
            worker_id: worker_id.to_string(),
            worker_kind: WorkerName::Function,
            log_patterns: vec![format!("fn*.{LOG_EXTENSION}")],
            casts: vec!["1".to_string()],
        };

        let paths: Vec<PathBuf> =
            temp_env::async_with_vars([(WORKSPACE_URI_ENV, Some(workspace.as_str()))], async {
                resolve_worker_log_files(&target).unwrap()
            })
            .await;

        assert_eq!(paths.len(), 1);
        assert!(
            paths[0]
                .to_string_lossy()
                .contains(&format!("{worker_id}_1"))
        );
    }
}
