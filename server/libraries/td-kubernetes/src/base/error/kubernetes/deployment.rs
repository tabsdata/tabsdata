//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeDeploymentError {
    #[error("Could not apply deployment in namespace '{namespace}': {cause}.")]
    ApplyFailed { namespace: String, cause: String } = 5000,
    #[error("Could not await deployment '{name}' in namespace '{namespace}': {cause}.")]
    AwaitFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5001,
    #[error(
        "Could not delete deployments matching '{label_selector}' in namespace '{namespace}': \
         {cause}."
    )]
    DeleteCollectionFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5002,
    #[error(
        "Could not list deployments matching '{label_selector}' in namespace '{namespace}': \
         {cause}."
    )]
    ListFailed {
        namespace: String,
        label_selector: String,
        cause: String,
    } = 5003,
    #[error("Deployment from '{key}' was created without a metadata.name.")]
    MissingName { key: String } = 5004,
    #[error("Deployment '{name}' was deleted before becoming available.")]
    DeletedBeforeAvailable { name: String } = 5005,
    #[error("Deployment '{name}' failed to progress.")]
    Failed { name: String } = 5006,
    #[error("Could not delete deployment '{name}' in namespace '{namespace}': {cause}.")]
    DeleteFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5007,
    #[error("Deployment '{name}' in namespace '{namespace}' has no pod selector.")]
    MissingSelector { namespace: String, name: String } = 5008,
    #[error(
        "Could not resolve the pod of deployment '{name}' in namespace '{namespace}': {cause}."
    )]
    PodResolutionFailed {
        namespace: String,
        name: String,
        cause: String,
    } = 5009,
}
