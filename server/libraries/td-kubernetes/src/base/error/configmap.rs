//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum ConfigMapError {
    #[error("ConfigMap '{name}' already exists in namespace '{namespace}'")]
    AlreadyExists { namespace: String, name: String } = 100,
    #[error("ConfigMap '{name}' not found in namespace '{namespace}'")]
    NotFound { namespace: String, name: String } = 1000,
    #[error("Could not query ConfigMap '{name}' in namespace '{namespace}': {cause}")]
    QueryFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5000,
    #[error("Could not create ConfigMap '{name}' in namespace '{namespace}': {cause}")]
    CreateFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error("Could not delete ConfigMap '{name}' in namespace '{namespace}': {cause}")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5002,
}

#[cfg(test)]
mod tests;
