//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubePodContextError {
    #[error("Required environment variable '{0}' is not set.")]
    EnvironmentVariableMissing(String) = 100,
    #[error("Namespace file '{0}' not found.")]
    NamespaceFileMissing(String) = 1000,
    #[error("Could not read namespace file '{path}': {cause}.")]
    NamespaceFileReadFailed { path: String, cause: String } = 5000,
}
