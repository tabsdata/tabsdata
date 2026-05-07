//
// Copyright 2026 Tabs Data Inc.
//

use std::path::{Path, PathBuf};

const KM_CONFIG_FOLDER: &str = "config";
const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";

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
}
