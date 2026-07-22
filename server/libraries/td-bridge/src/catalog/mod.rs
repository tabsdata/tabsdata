//
// Copyright 2026 Tabsdata Inc.
//

//! The `ServerCatalog` port: how the server reads interchange (etc) files,
//! vended by [`BridgeProvider::server_catalog`](crate::BridgeProvider::server_catalog).
//! [`etc_folder`] is the local etc-folder impl; the kubernetes catalog lives
//! in `td-kubernetes`.

pub mod etc_folder;

use async_trait::async_trait;
use std::fmt;
use std::fmt::Formatter;
use std::io::Error;
use std::path::PathBuf;
use td_error::td_error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum EtcContent {
    ServerVersion_yaml,
    AllowedPythonVersions_yaml,
    AvailablePythonVersions_yaml,
    ValidPythonVersions_yaml,
    ServerBuildManifest_yaml,
}

impl EtcContent {
    fn file(&self) -> &'static str {
        match self {
            EtcContent::ServerVersion_yaml => "server-version.yaml",
            EtcContent::AllowedPythonVersions_yaml => "allowed-python-versions.yaml",
            EtcContent::AvailablePythonVersions_yaml => "available-python-versions.yaml",
            EtcContent::ValidPythonVersions_yaml => "valid-python-versions.yaml",
            EtcContent::ServerBuildManifest_yaml => "server-build-manifest.yaml",
        }
    }
}

impl fmt::Display for EtcContent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            EtcContent::ServerVersion_yaml => "Server Version",
            EtcContent::AllowedPythonVersions_yaml => "Allowed Python Versions",
            EtcContent::AvailablePythonVersions_yaml => "Available Python Versions",
            EtcContent::ValidPythonVersions_yaml => "Valid Python Versions",
            EtcContent::ServerBuildManifest_yaml => "Server Build Manifest",
        };
        write!(f, "{name}")
    }
}

#[td_error]
pub enum EtcError {
    #[error("Error creating the server catalog instance '{location}': {cause}")]
    EtcStoreLocationCreationError { location: PathBuf, cause: Error },
    #[error("Error reading a server catalog file '{location}'")]
    NotFoundError { location: PathBuf },
    #[error("Error reading a server catalog file '{location}': {cause}")]
    ReadError { location: PathBuf, cause: Error },
    #[error("Error serializing a server catalog content '{content}': {cause}")]
    SerializationError { content: EtcContent, cause: String },
}

#[async_trait]
pub trait ServerCatalog: Send + Sync {
    async fn read(&self, content: &EtcContent) -> Result<Option<Vec<u8>>, EtcError>;
}
