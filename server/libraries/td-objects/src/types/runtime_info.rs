//
// Copyright 2025. Tabs Data Inc.
//

use crate::types::basic::{BuildManifest, PythonVersion, TabsdataVersion};

#[td_type::Dto]
pub struct ServerVersion {
    version: TabsdataVersion,
}

impl Default for ServerVersion {
    fn default() -> Self {
        Self {
            version: TabsdataVersion::try_from("-unknown-").unwrap(),
        }
    }
}

#[derive(Default)]
#[td_type::Dto]
pub struct PythonVersions {
    #[builder(default)]
    versions: Vec<PythonVersion>,
}

#[td_type::Dto]
pub struct RuntimeInfo {
    version: TabsdataVersion,
    build_manifest: BuildManifest,
    python_versions: Vec<PythonVersion>,
}
