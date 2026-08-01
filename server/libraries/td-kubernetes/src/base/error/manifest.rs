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
}

#[cfg(test)]
mod tests;
