//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeSecretError {
    #[error("Secret '{name}' already exists in namespace '{namespace}'.")]
    AlreadyExists { namespace: String, name: String } = 100,
    #[error("Secret '{name}' not found in namespace '{namespace}'.")]
    NotFound { namespace: String, name: String } = 1000,
    #[error("Could not query Secret '{name}' in namespace '{namespace}': {cause}.")]
    QueryFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5000,
    #[error("Could not create Secret '{name}' in namespace '{namespace}': {cause}.")]
    CreateFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error("Could not replace Secret '{name}' in namespace '{namespace}': {cause}.")]
    ReplaceFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5002,
    #[error("Could not delete Secret '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5003,
    #[error("Could not list Secrets in namespace '{namespace}': {cause}.")]
    ListFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5004,
}
