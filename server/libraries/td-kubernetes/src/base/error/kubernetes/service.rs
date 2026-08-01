//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeServiceError {
    #[error("Could not apply service '{name}' in namespace '{namespace}': {cause}.")]
    ApplyFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5000,
    #[error("Could not replace service '{name}' in namespace '{namespace}': {cause}.")]
    ReplaceFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error("Could not query service '{name}' in namespace '{namespace}': {cause}.")]
    QueryFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5002,
    #[error("Could not delete service '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5003,
    #[error("Could not list services in namespace '{namespace}': {cause}.")]
    ListFailed { namespace: String, cause: String } = 5004,
    #[error("Could not list services of every namespace: {cause}.")]
    ListEverywhereFailed { cause: String } = 5006,
    #[error("Service manifest declares no metadata.name.")]
    MissingName = 5005,
}
