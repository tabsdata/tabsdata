//
// Copyright 2025 Tabs Data Inc.
//

use getset::Getters;
use td_common::server::etc_service;
use td_common::server::EtcContent::AvailablePythonVersions_yaml;
use td_error::{td_error, TdError};
use td_objects::types::runtime_info::RuntimeInfo;
use tracing::warn;

pub(crate) mod layers;
pub mod services;

#[td_error]
pub enum RuntimeInfoError {
    #[error("Failed to read runtime info {0}, error: {1}")]
    ReadError(String, String) = 0,
}

async fn runtime_info() -> Result<RuntimeInfo, TdError> {
    let data = etc_service()
        .await?
        .read(&AvailablePythonVersions_yaml)
        .await?;
    let info = match data {
        Some(data) => serde_yaml::from_slice::<RuntimeInfo>(&data).map_err(|err| {
            RuntimeInfoError::ReadError(AvailablePythonVersions_yaml.to_string(), err.to_string())
        })?,
        None => {
            warn!(
                "etc resource '{}' not found, using default runtime info",
                AvailablePythonVersions_yaml
            );
            RuntimeInfo::builder().build()?
        }
    };
    Ok(info)
}

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct RuntimeContext {
    info: RuntimeInfo,
}

impl RuntimeContext {
    pub async fn new() -> Result<Self, TdError> {
        let info = runtime_info().await?;
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
        assert!(res.unwrap().info().versions().is_empty());
    }
}
