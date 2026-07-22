//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

use crate::base::error::kubernetes::KubernetesError;

#[td_error]
pub enum ProfileError {
    #[error("Profile '{0}' not found.")]
    NotFound(String) = 0,
    #[error("Path component '{0}' in '{1}' contains characters not allowed in a profile path.")]
    InvalidPathComponent(String, String) = 1,
    #[error("Duplicate file name '{0}' encountered while loading the profile.")]
    DuplicateFile(String) = 2,
    #[error("Family folder '{0}' is missing at '{1}'.")]
    FamilyFolderMissing(String, String) = 3,
    #[error("Family folder '{0}' must be flat, but contains the subfolder '{1}'.")]
    FamilyFolderNotFlat(String, String) = 4,
    #[error("Profile is missing the '{0}/{1}' entry.")]
    EntryMissing(String, String) = 5,
    #[error("Unknown profile family '{0}'.")]
    UnknownFamily(String) = 6,
    #[error("Resolved profiles path '{0}' does not exist or is not a directory.")]
    KprofilesPathInvalid(String) = 1000,
    #[error("Could not locate the '{0}' binary: {1}.")]
    ResourceBinaryNotFound(String, String) = 5000,
    #[error("Could not run '{0}': {1}.")]
    ResourceBinaryFailed(String, String) = 5001,
    #[error("Output of '{0}' is not valid UTF-8.")]
    ResourceBinaryNonUtf8Output(String) = 5002,
    #[error("Could not resolve the tabsdata package version: {0}.")]
    TabsdataVersionResolveFailed(String) = 5003,
    #[error("Could not walk '{0}': {1}.")]
    WalkFailed(String, String) = 5004,
    #[error("Could not read '{0}': {1}.")]
    ReadFailed(String, String) = 5005,
    #[error("Could not serialize ConfigMap entry for '{0}': {1}.")]
    EntrySerializeFailed(String, String) = 5006,
    #[error("Command '{0}' failed with status {1}: {2}.")]
    ResourceBinaryStatus(String, String, String) = 5007,
    #[error("Could not stat '{0}': {1}.")]
    StatFailed(String, String) = 5008,
    #[error("Failed to resolve placeholders in '{0}/{1}': {2}.")]
    PlaceholderResolutionFailed(String, String, String) = 5009,
    #[error("Configmap '{0}' entry '{1}' is not valid UTF-8: {2}.")]
    ConfigmapEntryNotUtf8(String, String, String) = 5010,
    #[error("Failed to deserialize '{0}': {1}.")]
    ManifestDeserializeFailed(String, String) = 5011,
    #[error("Failed to serialize the profile manifest: {0}.")]
    ManifestSerializeFailed(String) = 5012,
    #[error(transparent)]
    Kubernetes(#[from] KubernetesError) = 5013,
}

#[cfg(test)]
mod tests;
