//
// Copyright 2026 Tabsdata Inc.
//

//! Reads worker logs that AKS's Container Insights agent stored in the
//! `ContainerLogV2` table of a Log Analytics workspace, for workers whose pod is
//! gone. With Kubernetes metadata collection on, every row carries the pod's
//! labels, so a worker is found by its `tabsdata.com/log-id` label.
//!
//! The apiserver signs in with Azure Workload Identity. Its role assignment on
//! the workspace carries a condition limiting it to its own namespace's rows:
//! that is what keeps an instance from reading another's logs.

use std::path::PathBuf;
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
use crate::env::ENV_TD_WORKER_LOGS_WORKSPACE;
use crate::render::container_header;

const ENV_AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const ENV_AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
const ENV_AZURE_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
const ENV_AZURE_AUTHORITY_HOST: &str = "AZURE_AUTHORITY_HOST";
const ENV_POD_SERVICE_ACCOUNT: &str = "POD_SERVICE_ACCOUNT";
const AUTHORITY_URL: &str = "https://login.microsoftonline.com/";
const QUERY_URL: &str = "https://api.loganalytics.io/v1/";
const QUERY_SCOPE: &str = "https://api.loganalytics.io/.default";
const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
const TABLE: &str = "ContainerLogV2";
const QUERY_TIMESPAN: &str = "P730D";
const TIME_COLUMN: &str = "TimeGenerated";
const ITEM_COLUMN: &str = "_ItemId";
const CONTAINER_COLUMN: &str = "ContainerName";
const MESSAGE_COLUMN: &str = "LogMessage";
const METADATA_COLUMN: &str = "KubernetesMetadata";
const PAGE_SIZE: usize = 5000;
const MAX_ATTEMPTS: u32 = 5;
const RETRY_BASE: Duration = Duration::from_millis(500);
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(60);
const API_TIMEOUT: Duration = Duration::from_secs(120);

/// Where the reader talks to; Azure's public cloud in production, local in
/// tests.
#[derive(Clone, Debug)]
pub(crate) struct Endpoints {
    pub authority: String,
    pub query: String,
}

impl Endpoints {
    pub fn from_lookup(env: impl Fn(&str) -> Option<String>) -> Self {
        let authority = env(ENV_AZURE_AUTHORITY_HOST).unwrap_or_else(|| AUTHORITY_URL.to_owned());
        Self {
            authority: with_trailing_slash(authority),
            query: QUERY_URL.to_owned(),
        }
    }
}

/// The Azure Workload Identity the webhook gave the pod.
#[derive(Clone, Debug)]
pub(crate) struct Identity {
    pub client_id: String,
    pub tenant_id: String,
    pub token_file: PathBuf,
    pub subject: String,
}

#[derive(Clone)]
pub(crate) struct LogAnalyticsReader {
    inner: Arc<Inner>,
}

struct Inner {
    client: reqwest::Client,
    endpoints: Endpoints,
    identity: Identity,
    namespace: String,
    workspace: String,
    page_size: usize,
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
#[serde(default)]
struct TokenError {
    error: String,
    error_description: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct QueryResponse {
    tables: Vec<Table>,
    error: Option<ErrorDetail>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Table {
    columns: Vec<Column>,
    rows: Vec<Vec<Value>>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Column {
    name: String,
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
    innererror: Option<Box<ErrorDetail>>,
}

/// Where the previous page ended: rows sort by time, then by item id.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Cursor {
    pub time: String,
    pub item: String,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Row {
    pub cursor: Cursor,
    pub container: String,
    pub message: Value,
}

enum QueryFailure {
    Denied(String),
    NoWorkspace(String),
    NoMetadata(String),
    Failed(String),
}

impl LogAnalyticsReader {
    /// Builds a reader when the pod carries an Azure Workload Identity and
    /// `tabsdata.yaml` names the workspace; otherwise Log Analytics is not part
    /// of this instance.
    pub fn from_environment(namespace: &str) -> Option<Self> {
        let (identity, workspace) = settings(namespace, non_empty_env)?;
        match Self::new(
            Endpoints::from_lookup(non_empty_env),
            identity,
            namespace,
            &workspace,
            PAGE_SIZE,
            RETRY_BASE,
        ) {
            Ok(reader) => {
                info!(
                    "Worker logs of removed pods are read from the Log Analytics workspace \
                     '{workspace}'."
                );
                Some(reader)
            }
            Err(cause) => {
                warn!("Could not build the Log Analytics client: {cause}");
                None
            }
        }
    }

    pub fn new(
        endpoints: Endpoints,
        identity: Identity,
        namespace: &str,
        workspace: &str,
        page_size: usize,
        retry_base: Duration,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(API_TIMEOUT)
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                endpoints,
                identity,
                namespace: namespace.to_owned(),
                workspace: workspace.to_owned(),
                page_size,
                retry_base,
                token: Mutex::new(None),
            }),
        })
    }
}

impl ArchiveReader for LogAnalyticsReader {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        let inner = self.inner.clone();
        let worker_id = request.worker_id.to_owned();
        let log_id = request.log_id.to_owned();
        let started_on_millis = request.started_on_millis;

        Box::pin(async_stream::stream! {
            for (kind, name) in [("namespace", &inner.namespace), ("log id", &log_id)] {
                if !is_kube_name(name) {
                    yield Err(LogBridgeError::Backend {
                        cause: format!("the {kind} '{name}' is not a valid Kubernetes name"),
                    });
                    return;
                }
            }
            let mut after: Option<Cursor> = None;
            let mut container: Option<String> = None;
            let mut found = false;
            loop {
                let query = rows_query(
                    &inner.namespace,
                    &log_id,
                    started_on_millis,
                    after.as_ref(),
                    inner.page_size,
                );
                let rows = match inner.query_rows(&query).await {
                    Ok(rows) => rows,
                    Err(QueryFailure::Denied(message)) => {
                        yield Ok(denied(&worker_id, &inner.workspace, &message));
                        return;
                    }
                    Err(QueryFailure::NoWorkspace(message)) => {
                        yield Err(LogBridgeError::Backend {
                            cause: format!(
                                "the Log Analytics workspace '{}' was not found: {message}",
                                inner.workspace
                            ),
                        });
                        return;
                    }
                    Err(QueryFailure::NoMetadata(message)) => {
                        yield Err(LogBridgeError::Backend {
                            cause: format!(
                                "the Log Analytics workspace '{}' has no Kubernetes metadata in \
                                 {TABLE} (turn on metadata collection with podLabels in Container \
                                 Insights): {message}",
                                inner.workspace
                            ),
                        });
                        return;
                    }
                    Err(QueryFailure::Failed(cause)) => {
                        yield Err(LogBridgeError::Backend {
                            cause: format!(
                                "reading Log Analytics workspace '{}': {cause}",
                                inner.workspace
                            ),
                        });
                        return;
                    }
                };
                let full_page = rows.len() >= inner.page_size;
                for row in rows {
                    found = true;
                    if container.as_deref() != Some(row.container.as_str()) {
                        yield Ok(container_header(&row.container));
                        container = Some(row.container.clone());
                    }
                    yield Ok(Bytes::from(line_of(&row.message)));
                    after = Some(row.cursor);
                }
                if !full_page {
                    break;
                }
            }
            if !found {
                yield Ok(not_found(&worker_id));
            }
        })
    }
}

impl Inner {
    async fn query_rows(&self, query: &str) -> Result<Vec<Row>, QueryFailure> {
        let url = format!(
            "{}workspaces/{}/query",
            self.endpoints.query, self.workspace
        );
        let body = json!({ "query": query, "timespan": QUERY_TIMESPAN });
        let mut attempt = 1;
        let mut token_refreshed = false;
        loop {
            let token = self.token().await.map_err(QueryFailure::Failed)?;
            let response = self
                .client
                .post(&url)
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .map_err(|error| QueryFailure::Failed(error.to_string()))?;
            let status = response.status();
            if status.is_success() {
                let response = response
                    .json::<QueryResponse>()
                    .await
                    .map_err(|error| QueryFailure::Failed(error.to_string()))?;
                if let Some(error) = response.error {
                    return Err(QueryFailure::Failed(format!(
                        "partial result: {}",
                        error.describe()
                    )));
                }
                return rows_of(response.tables).map_err(QueryFailure::Failed);
            }
            let text = response.text().await.unwrap_or_default();
            let message = error_message(&text);
            match status {
                StatusCode::UNAUTHORIZED if !token_refreshed => {
                    token_refreshed = true;
                    *self.token.lock().await = None;
                }
                StatusCode::FORBIDDEN => return Err(QueryFailure::Denied(message)),
                StatusCode::NOT_FOUND => return Err(QueryFailure::NoWorkspace(message)),
                StatusCode::BAD_REQUEST if text.contains(METADATA_COLUMN) => {
                    return Err(QueryFailure::NoMetadata(message));
                }
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
                    if attempt < MAX_ATTEMPTS =>
                {
                    tokio::time::sleep(self.retry_base * 2u32.pow(attempt - 1)).await;
                    attempt += 1;
                }
                _ => return Err(QueryFailure::Failed(format!("{status}: {message}"))),
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
        let assertion = tokio::fs::read_to_string(&self.identity.token_file)
            .await
            .map_err(|error| {
                format!(
                    "reading the federated token '{}': {error}",
                    self.identity.token_file.display()
                )
            })?;
        let url = format!(
            "{}{}/oauth2/v2.0/token",
            self.endpoints.authority, self.identity.tenant_id
        );
        let form = [
            ("grant_type", "client_credentials"),
            ("client_id", self.identity.client_id.as_str()),
            ("scope", QUERY_SCOPE),
            ("client_assertion_type", CLIENT_ASSERTION_TYPE),
            ("client_assertion", assertion.trim()),
        ];
        let response = self
            .client
            .post(&url)
            .form(&form)
            .send()
            .await
            .map_err(|error| format!("requesting an access token: {error}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            let detail = match serde_json::from_str::<TokenError>(&text) {
                Ok(error) if !error.error.is_empty() => {
                    format!("{}: {}", error.error, error.error_description)
                }
                _ => text,
            };
            return Err(format!(
                "requesting an access token for client '{}' (its federated credential must have \
                 the subject '{}'): {status}: {detail}",
                self.identity.client_id, self.identity.subject
            ));
        }
        let response = response
            .json::<TokenResponse>()
            .await
            .map_err(|error| format!("reading the access token: {error}"))?;
        let value = response.access_token;
        *cached = Some(CachedToken {
            value: value.clone(),
            expires_at: Instant::now() + Duration::from_secs(response.expires_in),
        });
        debug!("Fetched a Log Analytics access token.");
        Ok(value)
    }
}

impl ErrorDetail {
    fn describe(&self) -> String {
        let mut parts = vec![self.message.clone()];
        let mut inner = self.innererror.as_deref();
        while let Some(detail) = inner {
            if !detail.message.is_empty() {
                parts.push(detail.message.clone());
            }
            inner = detail.innererror.as_deref();
        }
        parts.retain(|part| !part.is_empty());
        parts.join(": ")
    }
}

fn error_message(text: &str) -> String {
    match serde_json::from_str::<ErrorResponse>(text) {
        Ok(body) if !body.error.message.is_empty() => body.error.describe(),
        _ => text.to_owned(),
    }
}

pub(crate) fn settings(
    namespace: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Option<(Identity, String)> {
    let token_file = env(ENV_AZURE_FEDERATED_TOKEN_FILE)?;
    let Some(workspace) = env(ENV_TD_WORKER_LOGS_WORKSPACE) else {
        warn!(
            "The pod has an Azure identity but no Log Analytics workspace is set: worker logs are \
             read from pods only."
        );
        return None;
    };
    let (Some(client_id), Some(tenant_id)) = (env(ENV_AZURE_CLIENT_ID), env(ENV_AZURE_TENANT_ID))
    else {
        warn!(
            "The pod's Azure identity has no client or tenant ID: worker logs are read from pods \
             only."
        );
        return None;
    };
    let service_account =
        env(ENV_POD_SERVICE_ACCOUNT).unwrap_or_else(|| "<service account>".to_owned());
    let identity = Identity {
        client_id,
        tenant_id,
        token_file: PathBuf::from(token_file),
        subject: format!("system:serviceaccount:{namespace}:{service_account}"),
    };
    Some((identity, workspace))
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn with_trailing_slash(mut url: String) -> String {
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

pub(crate) fn is_kube_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
        })
}

pub(crate) fn rows_query(
    namespace: &str,
    log_id: &str,
    started_on_millis: Option<i64>,
    after: Option<&Cursor>,
    page_size: usize,
) -> String {
    let mut query = format!("{TABLE}\n");
    if let Some(started_on) = started_on_millis
        .map(|millis| millis.saturating_sub(STARTED_ON_LEEWAY_MILLIS))
        .and_then(DateTime::from_timestamp_millis)
    {
        query.push_str(&format!(
            "| where {TIME_COLUMN} >= datetime({})\n",
            started_on.to_rfc3339_opts(SecondsFormat::Millis, true)
        ));
    }
    query.push_str(&format!(
        "| where PodNamespace == {}\n\
         | where tostring({METADATA_COLUMN}.podLabels[{}]) == {}\n",
        quoted(namespace),
        quoted(LOG_ID_LABEL),
        quoted(log_id),
    ));
    if let Some(cursor) = after {
        let time = format!("datetime({})", cursor.time);
        query.push_str(&format!(
            "| where {TIME_COLUMN} > {time} or ({TIME_COLUMN} == {time} and {ITEM_COLUMN} > {})\n",
            quoted(&cursor.item)
        ));
    }
    query.push_str(&format!(
        "| project {TIME_COLUMN}, {ITEM_COLUMN}, {CONTAINER_COLUMN}, {MESSAGE_COLUMN}\n\
         | order by {TIME_COLUMN} asc, {ITEM_COLUMN} asc\n\
         | take {page_size}"
    ));
    query
}

fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn rows_of(tables: Vec<Table>) -> Result<Vec<Row>, String> {
    let Some(table) = tables.into_iter().next() else {
        return Ok(Vec::new());
    };
    let position = |name: &str| {
        table
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| format!("the result has no '{name}' column"))
    };
    let time = position(TIME_COLUMN)?;
    let item = position(ITEM_COLUMN)?;
    let container = position(CONTAINER_COLUMN)?;
    let message = position(MESSAGE_COLUMN)?;
    Ok(table
        .rows
        .into_iter()
        .map(|mut values| {
            let mut take = |index: usize| {
                values
                    .get_mut(index)
                    .map(Value::take)
                    .unwrap_or(Value::Null)
            };
            Row {
                cursor: Cursor {
                    time: text_of(take(time)),
                    item: text_of(take(item)),
                },
                container: text_of(take(container)),
                message: take(message),
            }
        })
        .collect())
}

fn text_of(value: Value) -> String {
    match value {
        Value::String(text) => text,
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn line_of(message: &Value) -> String {
    let mut line = match message {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    };
    if !line.ends_with('\n') {
        line.push('\n');
    }
    line
}

fn not_found(worker_id: &str) -> Bytes {
    Bytes::from(format!(
        "no logs found for worker '{worker_id}' in Log Analytics (they may not be ingested yet, a \
         few minutes after the pod ends, or may have expired)\n"
    ))
}

fn denied(worker_id: &str, workspace: &str, message: &str) -> Bytes {
    Bytes::from(format!(
        "no access to the Log Analytics workspace '{workspace}' to read the logs of worker \
         '{worker_id}' (check the Log Analytics Data Reader assignment and its condition): \
         {message}\n"
    ))
}

#[cfg(test)]
mod tests;
