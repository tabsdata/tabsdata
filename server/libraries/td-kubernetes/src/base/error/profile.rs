//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum ProfileError {
    #[error("Profile '{0}' not found")]
    NotFound(String) = 0,
    #[error("Path component '{0}' in '{1}' contains characters not allowed in a profile path")]
    InvalidPathComponent(String, String) = 1,
    #[error("Duplicate file name '{0}' encountered while loading the profile")]
    DuplicateFile(String) = 2,
    #[error("Resolved profiles path '{0}' does not exist or is not a directory")]
    KprofilesPathInvalid(String) = 1000,
    #[error("Could not locate the '{0}' binary: {1}")]
    ResourceBinaryNotFound(String, String) = 5000,
    #[error("Could not run '{0}': {1}")]
    ResourceBinaryFailed(String, String) = 5001,
    #[error("Output of '{0}' is not valid UTF-8")]
    ResourceBinaryNonUtf8Output(String) = 5002,
    #[error("Could not resolve the tabsdata package version: {0}")]
    TabsdataVersionResolveFailed(String) = 5003,
    #[error("Could not walk '{0}': {1}")]
    WalkFailed(String, String) = 5004,
    #[error("Could not read '{0}': {1}")]
    ReadFailed(String, String) = 5005,
    #[error("Could not serialize ConfigMap entry for '{0}': {1}")]
    EntrySerializeFailed(String, String) = 5006,
}

#[cfg(test)]
mod tests;
