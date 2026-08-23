//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum ManifestError {
    #[error(
        "Manifest entry '{path}' is at version '{found}'; running tabsdata SDK is '{running}'."
    )]
    VersionMismatch {
        path: String,
        found: String,
        running: String,
    } = 0,
    #[error(
        "Profile is incompatible with the running tabsdata SDK '{running}'. \
         The following entries differ on major.minor:\n{entries}\n\
         Run 'tdkserver upgrade <instance>' first."
    )]
    VersionMismatches { running: String, entries: String } = 1,
    #[error(
        "Instance '{instance}' was initialized with tabsdata '{found}'; \
         the running tabsdata SDK is '{running}'. Use the matching SDK version, \
         or re-initialize the instance."
    )]
    InstanceVersionMismatch {
        instance: String,
        found: String,
        running: String,
    } = 2,
    #[error("Manifest is missing entry '{0}'.")]
    EntryMissing(String) = 1000,
    #[error("Could not parse version '{0}': {1}.")]
    VersionParseFailed(String, String) = 5000,
    #[error("Could not parse manifest: {0}.")]
    ParseFailed(String) = 5001,
    #[error("Could not serialize manifest: {0}.")]
    SerializeFailed(String) = 5002,
    #[error("Could not resolve the tabsdata package version: {0}.")]
    TabsdataVersionResolveFailed(String) = 5003,
    #[error(
        "The manifest configmap '{configmap}' was not found; instance '{instance}' is not \
         initialized."
    )]
    ConfigmapMissing { instance: String, configmap: String } = 5004,
}

#[derive(Debug, Clone)]
pub struct EntryMismatch {
    pub path: String,
    pub version: String,
}

#[td_error]
pub enum VersionMismatch {
    #[error("Failed to parse version '{value}': {cause}.")]
    Parse { value: String, cause: String } = 5010,
    #[error(
        "Profile entries do not match the running tabsdata SDK version '{running}': {entries:?}."
    )]
    Mismatch {
        running: String,
        entries: Vec<EntryMismatch>,
    } = 5011,
}

#[cfg(test)]
mod tests;
