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
use td_common::server::{
    CAST_FOLDER, LOG_FOLDER, LOG_PATTERN, MESSAGE_PATTERN, PROC_FOLDER, WORKSPACE_URI_ENV,
    WORK_FOLDER,
};
use td_error::{td_error, TdError};
use td_objects::types::execution::WorkerMessageDB;
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
    Input(message): Input<WorkerMessageDB>,
) -> Result<Vec<PathBuf>, TdError> {
    let worker_path = env::var(WORKSPACE_URI_ENV).map_err(ReadWorkerLogsError::EnvVar)?;
    let pattern = PathBuf::from(worker_path)
        .join(WORK_FOLDER)
        .join(PROC_FOLDER)
        .join(EPHEMERAL.as_ref())
        .join(FUNCTION.as_ref())
        .join(WORK_FOLDER)
        .join(CAST_FOLDER)
        .join(format!("{}{}", message.id(), MESSAGE_PATTERN))
        .join(WORK_FOLDER)
        .join(LOG_FOLDER)
        .join(LOG_PATTERN);
    let pattern = pattern.to_str().ok_or(ReadWorkerLogsError::EmptyPattern)?;

    let mut paths = Vec::new();
    for entry in glob(pattern).map_err(ReadWorkerLogsError::Pattern)? {
        match entry {
            Ok(path) => paths.push(path),
            Err(e) => Err(ReadWorkerLogsError::Glob(e))?,
        }
    }
    paths.sort();

    Ok(paths)
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
