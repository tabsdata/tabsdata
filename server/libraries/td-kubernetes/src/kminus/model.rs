//
// Copyright 2026 Tabs Data Inc.
//

use std::path::{Path, PathBuf};

use crate::constants::{FOLDER_PROFILE, FOLDER_STAGING};

const KM_CONFIG_FOLDER: &str = "config";
const KM_DATA_FOLDER: &str = "data";
const KM_PIDS_FOLDER: &str = "pids";
const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";
const KM_KUBECONFIG_FILE: &str = "kubeconfig.yaml";
const KM_BIN: &str = "km";

pub struct KminusInstance {
    path: PathBuf,
}

impl KminusInstance {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sysconfig_path(&self) -> PathBuf {
        self.path.join(KM_CONFIG_FOLDER).join(KM_SYSCONFIG_FILE)
    }

    pub fn kubeconfig_path(&self) -> PathBuf {
        self.path.join(KM_CONFIG_FOLDER).join(KM_KUBECONFIG_FILE)
    }

    pub fn staging_path(&self) -> PathBuf {
        self.path.join(FOLDER_STAGING)
    }

    pub fn staging_profile_path(&self) -> PathBuf {
        self.staging_path().join(FOLDER_PROFILE)
    }

    pub fn pids_path(&self) -> PathBuf {
        self.path
            .join(KM_DATA_FOLDER)
            .join(KM_BIN)
            .join(KM_PIDS_FOLDER)
    }
}
