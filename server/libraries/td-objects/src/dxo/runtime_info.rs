//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{BuildManifest, PythonVersion, TabsdataVersion};

#[ta_objects::Dto]
pub struct ServerVersion {
    pub version: TabsdataVersion,
}

impl Default for ServerVersion {
    fn default() -> Self {
        Self {
            version: TabsdataVersion::try_from("-unknown-").unwrap(),
        }
    }
}

#[ta_objects::Dto]
#[derive(Default)]
pub struct PythonVersions {
    pub versions: Vec<PythonVersion>,
}

#[ta_objects::Dto]
pub struct RuntimeInfo {
    pub version: TabsdataVersion,
    pub build_manifest: BuildManifest,
    pub python_versions: Vec<PythonVersion>,
}
