//
// Copyright 2025 Tabsdata Inc.
//

use bytes::Bytes;
use futures_util::StreamExt;
use td_bridge::BridgeProviderHandle;
use td_bridge::worker::{WorkerLogStream, WorkerLogTarget};
use td_common::worker::WorkerName;
use td_error::TdError;
use td_objects::dxo::worker::{LogsExtension, WorkerDB};
use td_objects::types::basic::{LogsCastNumber, WorkerType};
use td_objects::types::stream::BoxedSyncStream;
use tokio::sync::mpsc;

fn worker_log_target(
    worker: &WorkerDB,
    extensions: &[LogsExtension],
    casts: &[LogsCastNumber],
) -> WorkerLogTarget {
    let worker_kind = match worker.base.worker_type {
        WorkerType::FunctionRunner => WorkerName::Function,
        WorkerType::CronTrigger => WorkerName::CronTrigger,
        WorkerType::StageTrigger => WorkerName::StageTrigger,
    };
    WorkerLogTarget {
        worker_id: worker.base.id.to_string(),
        worker_kind,
        log_patterns: extensions.iter().map(LogsExtension::glob_pattern).collect(),
        casts: casts.iter().map(|cast| cast.to_string()).collect(),
    }
}

const LOG_STREAM_CHANNEL_CAPACITY: usize = 16;

fn into_boxed_sync_stream(stream: WorkerLogStream) -> BoxedSyncStream {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, TdError>>(LOG_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            if sender.send(item.map_err(TdError::from)).await.is_err() {
                break;
            }
        }
    });
    let mut receiver = receiver;
    let receiver_stream = futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).fuse();
    BoxedSyncStream::new(receiver_stream)
}

pub async fn worker_logs(
    bridge: &BridgeProviderHandle,
    worker: &WorkerDB,
    extensions: &[LogsExtension],
    casts: &[LogsCastNumber],
) -> Result<BoxedSyncStream, TdError> {
    let target = worker_log_target(worker, extensions, casts);
    let stream = bridge.worker_handler().logs(&target).await?;
    Ok(into_boxed_sync_stream(stream))
}

pub async fn worker_logs_tail(
    bridge: &BridgeProviderHandle,
    worker: &WorkerDB,
    extensions: &[LogsExtension],
    casts: &[LogsCastNumber],
) -> Result<BoxedSyncStream, TdError> {
    let target = worker_log_target(worker, extensions, casts);
    let stream = bridge.worker_handler().logs_tail(&target).await?;
    Ok(into_boxed_sync_stream(stream))
}
