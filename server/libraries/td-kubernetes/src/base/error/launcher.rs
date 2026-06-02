//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

use crate::base::error::kubernetes::KubernetesError;

#[td_error]
pub enum LauncherError {
    #[error("Pod manifest 'pods/{key}' failed provider validation:\n{errors}.")]
    PodValidationFailed { key: String, errors: String } = 0,
    #[error("Failed to parse pod manifest 'pods/{key}' as YAML: {cause}.")]
    PodYamlParseFailed { key: String, cause: String } = 1,
    #[error("Pod manifest 'pods/{key}' is missing the required label '{label}'.")]
    PodMissingTierLabel { key: String, label: String } = 2,
    #[error("Pod manifest 'pods/{key}' has unknown tier label '{label}: {value}'.")]
    PodUnknownTierLabel {
        key: String,
        label: String,
        value: String,
    } = 3,
    #[error(
        "Failed to deserialize pod manifest 'pods/{key}' as the {tier} tier's expected kind: {cause}."
    )]
    PodDeserializeFailed {
        key: String,
        tier: String,
        cause: String,
    } = 4,
    #[error("Init job '{name}' failed.")]
    JobFailed { name: String } = 5,
    #[error("Regular pod '{name}' failed.")]
    PodFailed { name: String } = 6,
    #[error("Init job from '{key}' was created without a metadata.name.")]
    JobMissingName { key: String } = 5000,
    #[error("Regular pod from '{key}' was created without a metadata.name.")]
    PodMissingName { key: String } = 5001,
    #[error("Init job '{name}' was deleted before completion.")]
    JobDeletedBeforeCompletion { name: String } = 5002,
    #[error("Regular pod '{name}' was deleted before becoming ready.")]
    PodDeletedBeforeReady { name: String } = 5003,
    #[error(transparent)]
    Kubernetes(#[from] KubernetesError) = 5004,
}
