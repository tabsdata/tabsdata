//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeConfigMapError {
    #[error("ConfigMap '{name}' already exists in namespace '{namespace}'.")]
    AlreadyExists { namespace: String, name: String } = 100,
    #[error("ConfigMap '{name}' not found in namespace '{namespace}'.")]
    NotFound { namespace: String, name: String } = 1000,
    #[error("Could not query ConfigMap '{name}' in namespace '{namespace}': {cause}.")]
    QueryFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5000,
    #[error("Could not create ConfigMap '{name}' in namespace '{namespace}': {cause}.")]
    CreateFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error("Could not read ConfigMap '{name}' in namespace '{namespace}': {cause}.")]
    ReadFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5002,
    #[error("Could not delete ConfigMap '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5003,
    #[error("Could not list ConfigMaps in namespace '{namespace}': {cause}.")]
    ListFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5004,
    #[error("Could not replace ConfigMap '{name}' in namespace '{namespace}': {cause}.")]
    ReplaceFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5005,
}

#[cfg(test)]
mod tests;
