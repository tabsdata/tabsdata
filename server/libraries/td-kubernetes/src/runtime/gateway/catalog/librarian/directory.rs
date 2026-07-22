//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use serde::Serialize;
use td_bridge::catalog::{EtcContent, EtcError, ServerCatalog};

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

pub struct KubernetesServerCatalog;

impl KubernetesServerCatalog {
    pub fn new() -> Self {
        Self
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
            | EtcContent::ValidPythonVersions_yaml => {
                let bytes = serde_yaml::to_string(&PythonVersions::supported())
                    .map_err(|cause| EtcError::SerializationError {
                        content: *content,
                        cause: cause.to_string(),
                    })?
                    .into_bytes();
                Ok(Some(bytes))
            }
            EtcContent::ServerVersion_yaml | EtcContent::ServerBuildManifest_yaml => Ok(None),
        }
    }
}
