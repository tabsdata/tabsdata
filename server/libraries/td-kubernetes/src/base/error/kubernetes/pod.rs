//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubePodError {
    #[error("Could not apply pod in namespace '{namespace}': {cause}.")]
    ApplyFailed { namespace: String, cause: String } = 5000,
    #[error("Could not await pod '{name}' in namespace '{namespace}': {cause}.")]
    AwaitFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error(
        "Could not delete pods matching '{label_selector}' in namespace '{namespace}': {cause}."
    )]
    DeleteCollectionFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5002,
    #[error("Could not list pods matching '{label_selector}' in namespace '{namespace}': {cause}.")]
    ListFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5003,
    #[error("Pod from '{key}' was created without a metadata.name.")]
    MissingName { key: String } = 5004,
    #[error("Pod '{name}' was deleted before becoming ready.")]
    DeletedBeforeReady { name: String } = 5005,
    #[error("Pod '{name}' failed.")]
    Failed { name: String } = 5006,
    #[error("Could not delete pod '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5007,
    #[error("Could not stream logs of pod '{name}' in namespace '{namespace}': {cause}.")]
    LogStreamFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5008,
    #[error("Could not watch pod '{name}' in namespace '{namespace}': {cause}.")]
    WatchFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5009,
    #[error("No pod matching '{label_selector}' found in namespace '{namespace}'.")]
    NotFound {
        namespace: String,
        label_selector: String,
    } = 5010,
    #[error(
        "Expected a single pod matching '{label_selector}' in namespace '{namespace}', found {count}."
    )]
    MultipleFound {
        namespace: String,
        label_selector: String,
        count: usize,
    } = 5011,
}
