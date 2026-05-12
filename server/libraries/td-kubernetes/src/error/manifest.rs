//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum ManifestError {
    #[error("Manifest entry '{path}' is at version '{found}'; running tabsdata SDK is '{running}'")]
    VersionMismatch {
        path: String,
        found: String,
        running: String,
    } = 0,
    #[error("Manifest is missing entry '{0}'")]
    EntryMissing(String) = 1000,
    #[error("Could not parse version '{0}': {1}")]
    VersionParseFailed(String, String) = 5000,
    #[error("Could not parse manifest: {0}")]
    ParseFailed(String) = 5001,
    #[error("Could not serialize manifest: {0}")]
    SerializeFailed(String) = 5002,
}
