//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use serde::Serialize;
use std::process::Command;
use std::sync::OnceLock;
use td_bridge::catalog::{EtcContent, EtcError, ServerCatalog};
use td_python::python::resolve_python_binary;
use tracing::warn;

const TDINFO_PROGRAM: &str = "td_tdinfo";
const TDINFO_VERSION_KEY: &str = "version";
const TDINFO_SOURCETRACK_KEY: &str = "sourcetrack";

#[derive(Serialize)]
struct PythonVersions {
    versions: Vec<String>,
}

impl PythonVersions {
    fn supported() -> Self {
        Self {
            versions: vec!["3.12.13".to_owned()],
        }
    }
}

#[derive(Serialize)]
struct ServerVersion {
    version: String,
}

#[derive(Serialize)]
struct ServerBuildManifest {
    sourcetrack: serde_yaml::Value,
}

pub struct KubernetesServerCatalog;

impl KubernetesServerCatalog {
    pub fn new() -> Self {
        Self
    }

    fn tdinfo() -> Option<&'static serde_yaml::Mapping> {
        static INFO: OnceLock<Option<serde_yaml::Mapping>> = OnceLock::new();
        INFO.get_or_init(Self::run_tdinfo).as_ref()
    }

    fn run_tdinfo() -> Option<serde_yaml::Mapping> {
        let program = match resolve_python_binary(TDINFO_PROGRAM) {
            Ok(program) => program,
            Err(cause) => {
                warn!("Could not resolve '{TDINFO_PROGRAM}': {cause}");
                return None;
            }
        };
        let output = match Command::new(&program).output() {
            Ok(output) => output,
            Err(cause) => {
                warn!("Could not run '{}': {cause}", program.display());
                return None;
            }
        };
        if !output.status.success() {
            warn!(
                "'{}' failed with status {}: {}",
                program.display(),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
            return None;
        }
        match serde_yaml::from_slice::<serde_yaml::Mapping>(&output.stdout) {
            Ok(mapping) => Some(mapping),
            Err(cause) => {
                warn!(
                    "Could not parse the output of '{}': {cause}",
                    program.display()
                );
                None
            }
        }
    }

    fn server_version(
        content: &EtcContent,
        info: &serde_yaml::Mapping,
    ) -> Result<Option<Vec<u8>>, EtcError> {
        let Some(version) = info
            .get(TDINFO_VERSION_KEY)
            .and_then(serde_yaml::Value::as_str)
        else {
            return Ok(None);
        };
        Ok(Some(Self::serialize(
            content,
            &ServerVersion {
                version: version.to_owned(),
            },
        )?))
    }

    fn server_build_manifest(
        content: &EtcContent,
        info: &serde_yaml::Mapping,
    ) -> Result<Option<Vec<u8>>, EtcError> {
        match info.get(TDINFO_SOURCETRACK_KEY) {
            None | Some(serde_yaml::Value::Null) => Ok(None),
            Some(sourcetrack) => Ok(Some(Self::serialize(
                content,
                &ServerBuildManifest {
                    sourcetrack: sourcetrack.clone(),
                },
            )?)),
        }
    }

    fn serialize<T: Serialize>(content: &EtcContent, value: &T) -> Result<Vec<u8>, EtcError> {
        Ok(serde_yaml::to_string(value)
            .map_err(|cause| EtcError::SerializationError {
                content: *content,
                cause: cause.to_string(),
            })?
            .into_bytes())
    }
}

impl Default for KubernetesServerCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServerCatalog for KubernetesServerCatalog {
    async fn read(&self, content: &EtcContent) -> Result<Option<Vec<u8>>, EtcError> {
        match content {
            EtcContent::AllowedPythonVersions_yaml
            | EtcContent::AvailablePythonVersions_yaml
            | EtcContent::ValidPythonVersions_yaml => Ok(Some(Self::serialize(
                content,
                &PythonVersions::supported(),
            )?)),
            EtcContent::ServerVersion_yaml => match Self::tdinfo() {
                Some(info) => Self::server_version(content, info),
                None => Ok(None),
            },
            EtcContent::ServerBuildManifest_yaml => match Self::tdinfo() {
                Some(info) => Self::server_build_manifest(content, info),
                None => Ok(None),
            },
        }
    }
}

#[cfg(test)]
mod tests;
