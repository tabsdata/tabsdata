//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;

use crate::base::constants::LogLevel;
use crate::runtime::provider::ClusterProvider;

#[derive(Debug, Clone)]
pub struct Summary {
    pub provider: ClusterProvider,
    pub instance_name: Option<String>,
    pub instance_path: Option<PathBuf>,
    pub kubeconfig_path: Option<PathBuf>,
    pub context: Option<String>,
    pub cluster_url: Option<String>,
    pub cluster_name: Option<String>,
    pub namespace: Option<String>,
    pub loglevel: Option<LogLevel>,
}
