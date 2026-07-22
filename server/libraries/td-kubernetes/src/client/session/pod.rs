//
// Copyright 2026 Tabsdata Inc.
//

use std::env;
use std::fs;
use std::path::PathBuf;

use crate::base::error::kubernetes::KubePodContextError;

pub const NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
pub const POD_NAME_ENV: &str = "POD_NAME";
pub const POD_NAMESPACE_ENV: &str = "POD_NAMESPACE";
pub const POD_SERVICE_ACCOUNT_ENV: &str = "POD_SERVICE_ACCOUNT";
pub const POD_TABSDATA_INSTANCE_ENV: &str = "POD_TABSDATA_INSTANCE";

#[derive(Debug, Clone)]
pub struct PodContext {
    pub namespace: String,
    pub pod_name: String,
    pub service_account: String,
    pub tabsdata_instance: String,
}

impl PodContext {
    pub fn from_environment() -> Result<Self, KubePodContextError> {
        Ok(Self {
            namespace: read_namespace()?,
            pod_name: read_environment_variable(POD_NAME_ENV)?,
            service_account: read_environment_variable(POD_SERVICE_ACCOUNT_ENV)?,
            tabsdata_instance: read_environment_variable(POD_TABSDATA_INSTANCE_ENV)?,
        })
    }
}

fn read_namespace() -> Result<String, KubePodContextError> {
    if let Ok(value) = env::var(POD_NAMESPACE_ENV)
        && !value.is_empty()
    {
        return Ok(value);
    }
    let path = PathBuf::from(NAMESPACE_FILE);
    if !path.exists() {
        return Err(KubePodContextError::NamespaceFileMissing(
            NAMESPACE_FILE.to_owned(),
        ));
    }
    fs::read_to_string(&path)
        .map(|contents| contents.trim().to_owned())
        .map_err(|cause| KubePodContextError::NamespaceFileReadFailed {
            path: NAMESPACE_FILE.to_owned(),
            cause: cause.to_string(),
        })
}

fn read_environment_variable(name: &str) -> Result<String, KubePodContextError> {
    env::var(name).map_err(|_| KubePodContextError::EnvironmentVariableMissing(name.to_owned()))
}
