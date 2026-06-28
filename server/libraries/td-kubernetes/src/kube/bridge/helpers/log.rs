//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashSet;

use bytes::Bytes;
use futures::AsyncReadExt;
use td_bridge::worker::{LogBridgeError, WorkerLogStream};

use crate::base::constants::LOG_POD_POLL_INTERVAL;
use crate::base::labels::LABEL_WORKER_ID;
use crate::kube::api::LabelFilter;
use crate::kube::api::helpers::pod::{is_pod_terminal, started_container_names};
use crate::kube::api::pod::{get_pods, stream_pod_log};
use crate::kube::context::KubeContext;

const READ_BUFFER_LEN: usize = 64 * 1024;
const CONTAINER_HEADER_SEPARATOR: &str = "=";
const CONTAINER_HEADER_SEPARATOR_LEN: usize = 50;

fn container_header(container: &str) -> Bytes {
    let line = CONTAINER_HEADER_SEPARATOR.repeat(CONTAINER_HEADER_SEPARATOR_LEN);
    Bytes::from(format!("{line}\ncontainer: {container}\n{line}\n"))
}

pub fn stream_worker_logs(context: &KubeContext, worker_id: &str, follow: bool) -> WorkerLogStream {
    let context = context.clone();
    let worker_id = worker_id.to_owned();

    Box::pin(async_stream::stream! {
        let mut streamed: HashSet<String> = HashSet::new();
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
                    if follow {
                        tokio::time::sleep(LOG_POD_POLL_INTERVAL).await;
                        continue;
                    }
                    return;
                }
            };
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
