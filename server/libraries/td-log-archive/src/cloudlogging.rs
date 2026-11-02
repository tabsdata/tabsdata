//
// Copyright 2026 Tabsdata Inc.
//

//! Reads worker logs that GKE's own logging agent stored in Cloud Logging, for
//! workers whose pod is gone. GKE writes every container's output to the
//! `stdout` and `stderr` logs of the `k8s_container` resource and copies the
//! pod's labels onto each entry, so a worker is found by its
//! `tabsdata.com/log-id` label, the name of its workload.
//!
//! Each instance reads one log view, `tabsdata-<namespace>`, which its
//! identity is granted alone: that is what keeps an instance from reading
//! another's logs. The view lives in `_Default` (location `global`) unless
//! `tabsdata.yaml` names another bucket or location.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use td_bridge::worker::{LogBridgeError, WorkerLogStream};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::archive::{ArchiveReader, ArchiveRequest, LOG_ID_LABEL, STARTED_ON_LEEWAY_MILLIS};
use crate::env::{ENV_TD_WORKER_LOGS_BUCKET, ENV_TD_WORKER_LOGS_LOCATION};
use crate::render::container_header;

const METADATA_URL: &str = "http://metadata.google.internal/computeMetadata/v1/";
const LOGGING_URL: &str = "https://logging.googleapis.com/v2/";
const METADATA_FLAVOR_HEADER: &str = "Metadata-Flavor";
const METADATA_FLAVOR: &str = "Google";
const EMAIL_PATH: &str = "instance/service-accounts/default/email";
const TOKEN_PATH: &str = "instance/service-accounts/default/token";
const PROJECT_PATH: &str = "project/project-id";
const CLUSTER_NAME_PATH: &str = "instance/attributes/cluster-name";
const LIST_PATH: &str = "entries:list";
const GOOGLE_SERVICE_ACCOUNT_SUFFIX: &str = ".iam.gserviceaccount.com";
const DEFAULT_BUCKET: &str = "_Default";
const DEFAULT_LOCATION: &str = "global";
const VIEW_PREFIX: &str = "tabsdata-";
const GKE_POD_LABEL_PREFIX: &str = "k8s-pod/";
const CONTAINER_LABEL: &str = "container_name";
const PAGE_SIZE: u32 = 1000;
const MAX_ATTEMPTS: u32 = 5;
const RETRY_BASE: Duration = Duration::from_millis(500);
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(60);
const METADATA_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);
const API_TIMEOUT: Duration = Duration::from_secs(60);

/// Where the reader talks to; fixed in production, local in tests.
#[derive(Clone, Debug)]
pub(crate) struct Endpoints {
    pub metadata: String,
    pub logging: String,
}

impl Endpoints {
    fn google() -> Self {
        Self {
            metadata: METADATA_URL.to_owned(),
            logging: LOGGING_URL.to_owned(),
        }
    }
}

/// The bucket holding the instance's log view, as `tabsdata.yaml` sets it.
#[derive(Clone, Debug, Default)]
pub(crate) struct ViewPlace {
    pub bucket: Option<String>,
    pub location: Option<String>,
}

impl ViewPlace {
    fn from_environment() -> Self {
        Self {
            bucket: non_empty_env(ENV_TD_WORKER_LOGS_BUCKET),
            location: non_empty_env(ENV_TD_WORKER_LOGS_LOCATION),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CloudLoggingReader {
    inner: Arc<Inner>,
}

struct Inner {
    metadata: reqwest::Client,
    api: reqwest::Client,
    endpoints: Endpoints,
    cluster: String,
    namespace: String,
    view: String,
    retry_base: Duration,
    token: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    value: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ListResponse {
    entries: Vec<Entry>,
    next_page_token: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Entry {
    text_payload: Option<String>,
    json_payload: Option<Value>,
    resource: Resource,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Resource {
    labels: BTreeMap<String, String>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ErrorDetail {
    message: String,
}

enum ListFailure {
    Denied(String),
    Failed(String),
}

impl CloudLoggingReader {
    /// Builds a reader when the pod runs on GKE as a Google service account;
    /// with no metadata server, or only the Workload Identity pool identity,
    /// Cloud Logging is not part of this instance.
    pub async fn from_metadata(namespace: &str) -> Option<Self> {
        Self::discover(
            Endpoints::google(),
            namespace,
            ViewPlace::from_environment(),
            RETRY_BASE,
        )
        .await
    }

    pub async fn discover(
        endpoints: Endpoints,
        namespace: &str,
        place: ViewPlace,
        retry_base: Duration,
    ) -> Option<Self> {
        let metadata = match reqwest::Client::builder()
            .connect_timeout(METADATA_CONNECT_TIMEOUT)
            .timeout(METADATA_TIMEOUT)
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                warn!("Could not build the metadata client: {error}");
                return None;
            }
        };
        let email = match metadata_text(&metadata, &endpoints, EMAIL_PATH).await {
            Ok(email) => email,
            Err(cause) => {
                debug!("No GKE metadata server ({cause}): worker logs are read from pods only.");
                return None;
            }
        };
        if !email.ends_with(GOOGLE_SERVICE_ACCOUNT_SUFFIX) {
            debug!(
                "The pod's identity '{email}' is not a Google service account: worker logs are \
                 read from pods only."
            );
            return None;
        }
        let project = required_metadata(&metadata, &endpoints, PROJECT_PATH).await?;
        let cluster = required_metadata(&metadata, &endpoints, CLUSTER_NAME_PATH).await?;
        let api = match reqwest::Client::builder().timeout(API_TIMEOUT).build() {
            Ok(client) => client,
            Err(error) => {
                warn!("Could not build the Cloud Logging client: {error}");
                return None;
            }
        };
        let view = view_name(&project, namespace, &place);
        info!("Worker logs of removed pods are read from the Cloud Logging view '{view}'.");
        Some(Self {
            inner: Arc::new(Inner {
                metadata,
                api,
                endpoints,
                cluster,
                namespace: namespace.to_owned(),
                view,
                retry_base,
                token: Mutex::new(None),
            }),
        })
    }
}

impl ArchiveReader for CloudLoggingReader {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        let inner = self.inner.clone();
        let worker_id = request.worker_id.to_owned();
        let filter = entries_filter(
            &inner.cluster,
            &inner.namespace,
            request.log_id,
            request.started_on_millis,
        );

        Box::pin(async_stream::stream! {
            let mut page_token: Option<String> = None;
            let mut container: Option<String> = None;
            let mut found = false;
            loop {
                let page = match inner.list_page(&filter, page_token.take()).await {
                    Ok(page) => page,
                    Err(ListFailure::Denied(message)) => {
                        yield Ok(denied(&worker_id, &inner.view, &message));
                        return;
                    }
                    Err(ListFailure::Failed(cause)) => {
                        yield Err(LogBridgeError::Backend {
                            cause: format!("reading Cloud Logging view '{}': {cause}", inner.view),
                        });
                        return;
                    }
                };
                for entry in page.entries {
                    found = true;
                    let entry_container = entry
                        .resource
                        .labels
                        .get(CONTAINER_LABEL)
                        .cloned()
                        .unwrap_or_default();
                    if container.as_deref() != Some(entry_container.as_str()) {
                        yield Ok(container_header(&entry_container));
                        container = Some(entry_container);
                    }
                    yield Ok(Bytes::from(line_of(&entry)));
                }
                match page.next_page_token.filter(|token| !token.is_empty()) {
                    Some(token) => page_token = Some(token),
                    None => break,
                }
            }
            if !found {
                yield Ok(not_found(&worker_id));
            }
        })
    }
}

impl Inner {
    async fn list_page(
        &self,
        filter: &str,
        page_token: Option<String>,
    ) -> Result<ListResponse, ListFailure> {
        let mut body = json!({
            "resourceNames": [self.view],
            "filter": filter,
            "orderBy": "timestamp asc",
            "pageSize": PAGE_SIZE,
        });
        if let Some(token) = page_token {
            body["pageToken"] = Value::String(token);
        }
        let url = format!("{}{LIST_PATH}", self.endpoints.logging);
        let mut attempt = 1;
        let mut token_refreshed = false;
        loop {
            let token = self.token().await.map_err(ListFailure::Failed)?;
            let response = self
                .api
                .post(&url)
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .map_err(|error| ListFailure::Failed(error.to_string()))?;
            let status = response.status();
            if status.is_success() {
                return response
                    .json::<ListResponse>()
                    .await
                    .map_err(|error| ListFailure::Failed(error.to_string()));
            }
            let message = error_message(response).await;
            match status {
                StatusCode::UNAUTHORIZED if !token_refreshed => {
                    token_refreshed = true;
                    *self.token.lock().await = None;
                }
                StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => {
                    return Err(ListFailure::Denied(message));
                }
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
                    if attempt < MAX_ATTEMPTS =>
                {
                    tokio::time::sleep(self.retry_base * 2u32.pow(attempt - 1)).await;
                    attempt += 1;
                }
                _ => return Err(ListFailure::Failed(format!("{status}: {message}"))),
            }
        }
    }

    async fn token(&self) -> Result<String, String> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref()
            && token.expires_at > Instant::now() + TOKEN_REFRESH_MARGIN
        {
            return Ok(token.value.clone());
        }
        let url = format!("{}{TOKEN_PATH}", self.endpoints.metadata);
        let response = self
            .metadata
            .get(&url)
            .header(METADATA_FLAVOR_HEADER, METADATA_FLAVOR)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| format!("fetching an access token: {error}"))?
            .json::<TokenResponse>()
            .await
            .map_err(|error| format!("reading the access token: {error}"))?;
        let value = response.access_token;
        *cached = Some(CachedToken {
            value: value.clone(),
            expires_at: Instant::now() + Duration::from_secs(response.expires_in),
        });
        Ok(value)
    }
}

async fn metadata_text(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    path: &str,
) -> Result<String, String> {
    client
        .get(format!("{}{path}", endpoints.metadata))
        .header(METADATA_FLAVOR_HEADER, METADATA_FLAVOR)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| error.to_string())?
        .text()
        .await
        .map(|text| text.trim().to_owned())
        .map_err(|error| error.to_string())
}

async fn required_metadata(
    client: &reqwest::Client,
    endpoints: &Endpoints,
    path: &str,
) -> Option<String> {
    match metadata_text(client, endpoints, path).await {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) | Err(_) => {
            warn!("The GKE metadata server has no '{path}': worker logs are read from pods only.");
            None
        }
    }
}

async fn error_message(response: reqwest::Response) -> String {
    let text = response.text().await.unwrap_or_default();
    match serde_json::from_str::<ErrorResponse>(&text) {
        Ok(body) if !body.error.message.is_empty() => body.error.message,
        _ => text,
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

pub(crate) fn view_name(project: &str, namespace: &str, place: &ViewPlace) -> String {
    let location = place.location.as_deref().unwrap_or(DEFAULT_LOCATION);
    let bucket = place.bucket.as_deref().unwrap_or(DEFAULT_BUCKET);
    format!(
        "projects/{project}/locations/{location}/buckets/{bucket}/views/{VIEW_PREFIX}{namespace}"
    )
}

/// The entry label GKE derives from a pod label: prefixed, with every `.` of
/// the key turned into `_` (the `/` is kept).
pub(crate) fn gke_pod_label(key: &str) -> String {
    format!("{GKE_POD_LABEL_PREFIX}{}", key.replace('.', "_"))
}

pub(crate) fn entries_filter(
    cluster: &str,
    namespace: &str,
    log_id: &str,
    started_on_millis: Option<i64>,
) -> String {
    let mut filter = format!(
        "(log_id(\"stdout\") OR log_id(\"stderr\")) \
         AND resource.type=\"k8s_container\" \
         AND resource.labels.cluster_name={} \
         AND resource.labels.namespace_name={} \
         AND labels.{}={}",
        quoted(cluster),
        quoted(namespace),
        quoted(&gke_pod_label(LOG_ID_LABEL)),
        quoted(log_id),
    );
    if let Some(started_on) = started_on_millis
        .map(|millis| millis.saturating_sub(STARTED_ON_LEEWAY_MILLIS))
        .and_then(DateTime::from_timestamp_millis)
    {
        filter.push_str(&format!(
            " AND timestamp>=\"{}\"",
            started_on.to_rfc3339_opts(SecondsFormat::Millis, true)
        ));
    }
    filter
}

fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn line_of(entry: &Entry) -> String {
    let mut line = match (&entry.text_payload, &entry.json_payload) {
        (Some(text), _) => text.clone(),
        (None, Some(payload)) => payload.to_string(),
        (None, None) => String::new(),
    };
    if !line.ends_with('\n') {
        line.push('\n');
    }
    line
}

fn not_found(worker_id: &str) -> Bytes {
    Bytes::from(format!(
        "no logs found for worker '{worker_id}' in Cloud Logging (they may have expired, or the \
         cluster's logging may not include workloads)\n"
    ))
}

fn denied(worker_id: &str, view: &str, message: &str) -> Bytes {
    Bytes::from(format!(
        "no access to the Cloud Logging view '{view}' to read the logs of worker '{worker_id}' \
         (check that the view exists in that bucket and roles/logging.viewAccessor is granted \
         on it): {message}\n"
    ))
}

#[cfg(test)]
mod tests;
