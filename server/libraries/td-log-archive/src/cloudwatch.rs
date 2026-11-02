//
// Copyright 2026 Tabsdata Inc.
//

//! Reads worker logs that Fluent Bit shipped to CloudWatch Logs, for workers
//! whose pod is gone. The layout is fixed: one log group per instance,
//! `/tabsdata/<namespace>`, and one stream per workload named after the
//! `tabsdata.com/log-id` label its pods carry. Each event holds the Fluent Bit
//! record, which names the container it came from.
//!
//! Only the worker's own stream is read, so the cost depends on the worker's
//! logs and not on how much the rest of the instance logged since.

use std::error::Error;

use aws_config::BehaviorVersion;
use aws_config::retry::RetryConfig;
use aws_sdk_cloudwatchlogs::Client;
use aws_sdk_cloudwatchlogs::config::http::HttpResponse;
use aws_sdk_cloudwatchlogs::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_cloudwatchlogs::operation::get_log_events::GetLogEventsError;
use bytes::Bytes;
use serde::Deserialize;
use td_bridge::worker::{LogBridgeError, WorkerLogStream};
use tracing::{debug, warn};

use crate::archive::{ArchiveReader, ArchiveRequest};
use crate::render::container_header;

const ENV_AWS_ROLE_ARN: &str = "AWS_ROLE_ARN";
const ENV_AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const LOG_GROUP_PREFIX: &str = "/tabsdata/";
const ACCESS_DENIED_CODE: &str = "AccessDeniedException";
const MAX_ATTEMPTS: u32 = 5;

#[derive(Clone)]
pub(crate) struct CloudWatchLogReader {
    client: Client,
    log_group: String,
}

#[derive(Deserialize)]
struct FluentBitRecord {
    log: String,
    #[serde(default)]
    kubernetes: Option<KubernetesMetadata>,
}

#[derive(Deserialize)]
struct KubernetesMetadata {
    container_name: Option<String>,
}

/// One archived log line and the container that wrote it, when known.
pub(crate) struct ArchivedLine {
    pub container: Option<String>,
    pub line: String,
}

impl CloudWatchLogReader {
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            client,
            log_group: format!("{LOG_GROUP_PREFIX}{namespace}"),
        }
    }

    /// Whether the pod runs with an IAM role for its service account, which
    /// is what makes CloudWatch the archive of this instance.
    pub fn assumes_role() -> bool {
        std::env::var_os(ENV_AWS_ROLE_ARN).is_some()
            && std::env::var_os(ENV_AWS_WEB_IDENTITY_TOKEN_FILE).is_some()
    }

    /// Builds a reader when the pod runs with an IAM role for its service
    /// account; without one, CloudWatch is not part of this instance.
    pub async fn from_environment(namespace: &str) -> Option<Self> {
        if !Self::assumes_role() {
            debug!("No IAM role for the service account: worker logs are read from pods only.");
            return None;
        }
        let config = aws_config::defaults(BehaviorVersion::latest())
            .retry_config(RetryConfig::standard().with_max_attempts(MAX_ATTEMPTS))
            .load()
            .await;
        if config.region().is_none() {
            warn!("An IAM role is set but no AWS region is: worker logs are read from pods only.");
            return None;
        }
        Some(Self::new(Client::new(&config), namespace))
    }
}

impl ArchiveReader for CloudWatchLogReader {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        let client = self.client.clone();
        let log_group = self.log_group.clone();
        let worker_id = request.worker_id.to_owned();
        let log_id = request.log_id.to_owned();

        Box::pin(async_stream::stream! {
            let mut token: Option<String> = None;
            let mut container: Option<String> = None;
            let mut found = false;
            loop {
                let page = client
                    .get_log_events()
                    .log_group_name(&log_group)
                    .log_stream_name(&log_id)
                    .start_from_head(true)
                    .set_next_token(token.clone())
                    .send()
                    .await;
                let page = match page {
                    Ok(page) => page,
                    Err(error) => {
                        let missing = error
                            .as_service_error()
                            .is_some_and(GetLogEventsError::is_resource_not_found_exception);
                        yield unavailable(&error, missing, &worker_id, &log_group);
                        return;
                    }
                };
                for event in page.events() {
                    found = true;
                    let archived = archived_line(event.message().unwrap_or_default());
                    if archived.container.is_some() && archived.container != container {
                        container = archived.container;
                        if let Some(name) = &container {
                            yield Ok(container_header(name));
                        }
                    }
                    yield Ok(Bytes::from(archived.line));
                }
                let next = page.next_forward_token().map(str::to_owned);
                if next.is_none() || next == token {
                    break;
                }
                token = next;
            }
            if !found {
                yield Ok(not_found(&worker_id, &log_group));
            }
        })
    }
}

pub(crate) fn archived_line(message: &str) -> ArchivedLine {
    let (container, mut line) = match serde_json::from_str::<FluentBitRecord>(message) {
        Ok(record) => (
            record
                .kubernetes
                .and_then(|metadata| metadata.container_name),
            record.log,
        ),
        Err(_) => (None, message.to_owned()),
    };
    if !line.ends_with('\n') {
        line.push('\n');
    }
    ArchivedLine { container, line }
}

fn not_found(worker_id: &str, log_group: &str) -> Bytes {
    Bytes::from(format!(
        "no logs found for worker '{worker_id}' in CloudWatch log group '{log_group}'\n"
    ))
}

fn unavailable<E>(
    error: &SdkError<E, HttpResponse>,
    missing: bool,
    worker_id: &str,
    log_group: &str,
) -> Result<Bytes, LogBridgeError>
where
    E: Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    if missing {
        return Ok(not_found(worker_id, log_group));
    }
    match error.code() {
        Some(ACCESS_DENIED_CODE) => Ok(Bytes::from(format!(
            "no access to CloudWatch log group '{log_group}' to read the logs of worker \
             '{worker_id}': {}\n",
            error.message().unwrap_or_default()
        ))),
        _ => Err(LogBridgeError::Backend {
            cause: format!(
                "reading CloudWatch log group '{log_group}': {}",
                DisplayErrorContext(error)
            ),
        }),
    }
}

#[cfg(test)]
mod tests;
