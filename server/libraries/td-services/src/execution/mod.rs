//
// Copyright 2025 Tabs Data Inc.
//

use getset::Getters;
use serde::de::DeserializeOwned;
use td_common::server::{EtcContent, etc_service};
use td_error::{TdError, td_error};
use td_objects::types::basic::{BuildManifest, TabsdataVersion};
use td_objects::types::runtime_info::{PythonVersions, RuntimeInfo, ServerVersion};
use tracing::error;

pub(crate) mod layers;
pub mod services;

#[td_error]
pub enum RuntimeInfoError {
    #[error("Failed to Deserialize runtime info {0}, error: {1}")]
    ReadError(String, String) = 0,
}

async fn deserialize<T: DeserializeOwned + Default>(
    etc_content: &EtcContent,
) -> Result<T, TdError> {
    let data = etc_service().await?.read(etc_content).await?;
    let data = match data {
        Some(data) => serde_yaml::from_slice::<T>(&data)
            .map_err(|err| RuntimeInfoError::ReadError(etc_content.to_string(), err.to_string()))?,
        None => {
            error!(
                "etc resource '{}' not found, using default value",
                etc_content
            );
            T::default()
        }
    };
    Ok(data)
}

async fn read_string(etc_content: &EtcContent) -> Result<String, TdError> {
    let data = etc_service().await?.read(etc_content).await?;
    let data = match data {
        Some(data) => String::from_utf8(data)
            .map_err(|err| RuntimeInfoError::ReadError(etc_content.to_string(), err.to_string()))?,
        None => {
            error!(
                "etc resource '{}' not found, using default value",
                etc_content
            );
            String::default()
        }
    };
    Ok(data)
}

async fn server_version() -> Result<TabsdataVersion, TdError> {
    let server_version = deserialize::<ServerVersion>(&EtcContent::ServerVersion_yaml).await?;
    Ok(server_version.version().clone())
}
async fn valid_python_versions() -> Result<PythonVersions, TdError> {
    let valid_python_versions =
        deserialize::<PythonVersions>(&EtcContent::ValidPythonVersions_yaml).await?;
    Ok(valid_python_versions)
}

async fn build_manifest() -> Result<BuildManifest, TdError> {
    let manifest: BuildManifest = read_string(&EtcContent::ServerBuildManifest_yaml)
        .await?
        .try_into()?;
    Ok(manifest)
}

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct RuntimeContext {
    info: RuntimeInfo,
}

impl RuntimeContext {
    pub async fn new() -> Result<Self, TdError> {
        let info = RuntimeInfo::builder()
            .version(server_version().await?)
            .build_manifest(build_manifest().await?)
            .python_versions(valid_python_versions().await?.versions().clone())
            .build()?;
        Ok(Self { info })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_info() {
        let res = RuntimeContext::new().await;
        assert!(res.is_ok());
        assert!(res.unwrap().info().python_versions().is_empty());
    }
}
