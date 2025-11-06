//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{BuildManifest, PythonVersion, TabsdataVersion};

#[td_type::Dto]
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

#[td_type::Dto]
#[derive(Default)]
pub struct PythonVersions {
    #[builder(default)]
    pub versions: Vec<PythonVersion>,
}

#[td_type::Dto]
pub struct RuntimeInfo {
    pub version: TabsdataVersion,
    pub build_manifest: BuildManifest,
    pub python_versions: Vec<PythonVersion>,
}
