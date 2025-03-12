//
// Copyright 2025 Tabs Data Inc.
//

use bytes::{Bytes, BytesMut};
use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use futures_util::{stream, TryStreamExt};
use td_error::td_error;
use td_error::TdError;
use td_objects::datasets::dlo::{BoxedSyncStream, WorkerLogPaths};
use td_tower::extractors::Input;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

const PATH_SEPARATOR: &str = "-----------------------------------------------";
const LOG_SEPARATOR: &str = "===============================================";

pub async fn get_worker_logs(
    Input(paths): Input<WorkerLogPaths>,
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
                .map_err(|e| TdError::from(ReadWorkerLogsError::IoError(e)));

            // Stream the path as the first item
            let path_stream = stream::once(async move {
                let path_bytes = Bytes::from(format!("{}\n", path.display()));
                Ok(path_bytes)
            });

            // Separator stream
            let path_separator_stream = stream::once(async move {
                let path_bytes = Bytes::from(format!("{}\n", PATH_SEPARATOR));
                Ok(path_bytes)
            });

            // Separator stream
            let log_separator_stream = stream::once(async move {
                let path_bytes = Bytes::from(format!("{}\n", LOG_SEPARATOR));
                Ok(path_bytes)
            });

            // Combine the path stream and file stream
            let combined_stream = path_stream
                .chain(path_separator_stream)
                .chain(file_stream)
                .chain(log_separator_stream);
            Ok::<_, TdError>(combined_stream)
        };

        // Push the stream to the ordered stream futures.
        ordered_stream.push_back(Box::pin(stream));
    }

    // Flattening the ordered stream preserves the order of the streams.
    let aggregated_stream = ordered_stream.try_flatten();
    Ok(BoxedSyncStream::new(aggregated_stream))
}

#[td_error]
enum ReadWorkerLogsError {
    #[error("Failed to read worker log")]
    IoError(#[from] std::io::Error) = 5000,
}
