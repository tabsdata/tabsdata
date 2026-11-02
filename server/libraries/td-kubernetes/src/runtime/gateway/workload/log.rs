//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashSet;

use bytes::Bytes;
use futures::{AsyncReadExt, StreamExt};
use td_bridge::worker::{LogBridgeError, WorkerLogStream};
use td_log_archive::{ArchiveRequest, LogArchive, container_header};
use td_objects::types::basic::WorkerId;
use tokio::time::Instant;

use crate::base::constants::{LOG_POD_APPEARANCE_TIMEOUT, LOG_POD_POLL_INTERVAL};
use crate::base::labels::LABEL_WORKER_ID;
use crate::base::naming::logical_id_text;
use crate::kube::api::domain::labels::LabelFilter;
use crate::kube::api::entity::pod::status::{is_pod_terminal, started_container_names};
use crate::kube::api::entity::pod::{get_pods, stream_pod_log};
use crate::kube::context::KubeContext;

const READ_BUFFER_LEN: usize = 64 * 1024;
pub struct PodlessLogs {
    pub finished: bool,
    pub log_id: String,
    pub archive: Option<LogArchive>,
}

pub fn stream_worker_logs(
    context: &KubeContext,
    worker_id: &WorkerId,
    follow: bool,
    podless: PodlessLogs,
) -> WorkerLogStream {
    let context = context.clone();
    let worker_id = logical_id_text(worker_id);

    Box::pin(async_stream::stream! {
        let mut streamed: HashSet<String> = HashSet::new();
        let mut seen_pod = false;
        let waiting_since = Instant::now();
        loop {
            let label = LabelFilter::new(LABEL_WORKER_ID, &worker_id);
            let pods = match get_pods(&context, Some(label)).await {
                Ok(pods) => pods,
                Err(error) => {
                    yield Err(LogBridgeError::Backend { cause: error.to_string() });
                    return;
                }
            };
            let pod = match pods.into_iter().next() {
                Some(pod) => pod,
                None => {
                    if seen_pod {
                        return;
                    }
                    let awaiting_pod = follow
                        && !podless.finished
                        && waiting_since.elapsed() < LOG_POD_APPEARANCE_TIMEOUT;
                    if awaiting_pod {
                        tokio::time::sleep(LOG_POD_POLL_INTERVAL).await;
                        continue;
                    }
                    match &podless.archive {
                        Some(archive) => {
                            let mut archived = archive.stream(ArchiveRequest {
                                worker_id: &worker_id,
                                log_id: &podless.log_id,
                            });
                            while let Some(item) = archived.next().await {
                                yield item;
                            }
                        }
                        None => {
                            yield Ok(Bytes::from(format!(
                                "no pod found for worker '{worker_id}': its logs are no \
                                 longer retained (the pod was removed after the job's \
                                 retention window elapsed)\n"
                            )));
                        }
                    }
                    return;
                }
            };
            seen_pod = true;
            let pod_name = match pod.metadata.name.as_deref() {
                Some(name) => name.to_owned(),
                None => return,
            };

            for container in started_container_names(&pod) {
                if !streamed.insert(container.clone()) {
                    continue;
                }
                yield Ok(container_header(&container));

                let mut reader = match stream_pod_log(&context, &pod_name, &container, follow).await {
                    Ok(reader) => reader,
                    Err(error) => {
                        yield Err(LogBridgeError::Backend { cause: error.to_string() });
                        continue;
                    }
                };
                let mut buffer = vec![0u8; READ_BUFFER_LEN];
                loop {
                    match reader.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(read) => yield Ok(Bytes::copy_from_slice(&buffer[..read])),
                        Err(error) => {
                            yield Err(LogBridgeError::Read { cause: error.to_string() });
                            break;
                        }
                    }
                }
            }

            if !follow || is_pod_terminal(&pod) {
                return;
            }
            tokio::time::sleep(LOG_POD_POLL_INTERVAL).await;
        }
    })
}

#[cfg(test)]
mod tests;
