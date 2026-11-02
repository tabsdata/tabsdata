//
// Copyright 2026 Tabsdata Inc.
//

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use reqwest::{Certificate, Identity, StatusCode};
use serde::Deserialize;
use td_bridge::worker::{LogBridgeError, WorkerLogStream};
use tracing::info;

use crate::archive::{ArchiveReader, ArchiveRequest};

const ENV_PRCONFIG: &str = "PRCONFIG";
const POD_LOGS_PATH: &str = "getPodLogs";
const LOG_POD_ID_PARAM: &str = "logPodId";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
pub(crate) struct PodRunnerConfig {
    url: String,
    server: ServerConfig,
    client: ClientConfig,
}

#[derive(Deserialize)]
struct ServerConfig {
    tls: TlsConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TlsConfig {
    certificate_authority_data: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ClientConfig {
    certificate_data: String,
    certificate_key_data: String,
}

impl PodRunnerConfig {
    pub(crate) async fn read(path: &Path) -> Result<Self, String> {
        let text = tokio::fs::read_to_string(path)
            .await
            .map_err(|error| format!("reading '{}': {error}", path.display()))?;
        serde_yaml::from_str(&text)
            .map_err(|error| format!("parsing '{}': {error}", path.display()))
    }

    pub(crate) fn pod_logs_url(&self) -> String {
        format!("{}/{POD_LOGS_PATH}", self.url.trim_end_matches('/'))
    }

    pub(crate) fn client(&self) -> Result<reqwest::Client, String> {
        let authority = Certificate::from_pem(&decoded(
            "certificate-authority-data",
            &self.server.tls.certificate_authority_data,
        )?)
        .map_err(|error| format!("loading the certificate authority: {error}"))?;
        let mut identity = decoded("certificate-data", &self.client.certificate_data)?;
        identity.push(b'\n');
        identity.extend(decoded(
            "certificate-key-data",
            &self.client.certificate_key_data,
        )?);
        let identity = Identity::from_pem(&identity)
            .map_err(|error| format!("loading the client certificate: {error}"))?;
        reqwest::Client::builder()
            .tls_certs_only([authority])
            .identity(identity)
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .map_err(|error| format!("building the pod runner client: {error}"))
    }
}

#[derive(Clone)]
pub(crate) struct PodRunnerReader {
    prconfig: PathBuf,
}

impl PodRunnerReader {
    pub fn from_environment() -> Option<Self> {
        let prconfig = std::env::var_os(ENV_PRCONFIG).filter(|value| !value.is_empty())?;
        let reader = Self::new(prconfig);
        info!(
            "Worker logs of removed pods are read from the pod runner configured in '{}'.",
            reader.prconfig.display()
        );
        Some(reader)
    }

    pub fn new(prconfig: impl Into<PathBuf>) -> Self {
        Self {
            prconfig: prconfig.into(),
        }
    }
}

impl ArchiveReader for PodRunnerReader {
    fn stream(&self, request: ArchiveRequest<'_>) -> WorkerLogStream {
        let prconfig = self.prconfig.clone();
        let worker_id = request.worker_id.to_owned();
        let log_id = request.log_id.to_owned();

        Box::pin(async_stream::stream! {
            let mut response = match fetch(&prconfig, &log_id).await {
                Ok(response) => response,
                Err(cause) => {
                    yield Err(LogBridgeError::Backend {
                        cause: format!("reading the logs of pod '{log_id}' from the pod runner: {cause}"),
                    });
                    return;
                }
            };
            let status = response.status();
            if status == StatusCode::NOT_FOUND {
                yield Ok(not_found(&worker_id));
                return;
            }
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                yield Err(LogBridgeError::Backend {
                    cause: format!(
                        "reading the logs of pod '{log_id}' from the pod runner: {status}: {}",
                        body.trim()
                    ),
                });
                return;
            }
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => yield Ok(chunk),
                    Ok(None) => break,
                    Err(error) => {
                        yield Err(LogBridgeError::Read {
                            cause: error.to_string(),
                        });
                        return;
                    }
                }
            }
        })
    }
}

async fn fetch(prconfig: &Path, log_id: &str) -> Result<reqwest::Response, String> {
    let config = PodRunnerConfig::read(prconfig).await?;
    config
        .client()?
        .get(config.pod_logs_url())
        .query(&[(LOG_POD_ID_PARAM, log_id)])
        .send()
        .await
        .map_err(|error| error.to_string())
}

fn decoded(field: &str, value: &str) -> Result<Vec<u8>, String> {
    STANDARD
        .decode(value.trim())
        .map_err(|error| format!("decoding '{field}': {error}"))
}

fn not_found(worker_id: &str) -> Bytes {
    Bytes::from(format!(
        "no logs kept for worker '{worker_id}' by the pod runner (they may have expired)\n"
    ))
}

#[cfg(test)]
mod tests;
