//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeJobError {
    #[error("Could not apply job in namespace '{namespace}': {cause}.")]
    ApplyFailed { namespace: String, cause: String } = 5000,
    #[error("Could not await job '{name}' in namespace '{namespace}': {cause}.")]
    AwaitFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error(
        "Could not delete jobs matching '{label_selector}' in namespace '{namespace}': {cause}."
    )]
    DeleteCollectionFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5002,
    #[error("Could not list jobs matching '{label_selector}' in namespace '{namespace}': {cause}.")]
    ListFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5003,
    #[error("Job from '{key}' was created without a metadata.name.")]
    MissingName { key: String } = 5004,
    #[error("Job '{name}' was deleted before completion.")]
    DeletedBeforeCompletion { name: String } = 5005,
    #[error("Job '{name}' failed.")]
    Failed { name: String } = 5006,
    #[error("Could not delete job '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5007,
}
