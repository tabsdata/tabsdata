//
// Copyright 2026 Tabsdata Inc.
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
    #[error("Failed to materialize pod manifest 'pods/{key}' for the {tier} tier: {cause}.")]
    PodMaterializeFailed {
        key: String,
        tier: String,
        cause: String,
    } = 5008,
    #[error("No pod found for component '{component}'.")]
    ComponentPodMissing { component: String } = 5009,
    #[error("Init job '{name}' failed.")]
    JobFailed { name: String } = 5,
    #[error("Regular pod '{name}' failed.")]
    PodFailed { name: String } = 6,
    #[error("Regular cronjob '{name}' failed.")]
    CronJobFailed { name: String } = 7,
    #[error("Init job from '{key}' was created without a metadata.name.")]
    JobMissingName { key: String } = 5000,
    #[error("Regular pod from '{key}' was created without a metadata.name.")]
    PodMissingName { key: String } = 5001,
    #[error("Init job '{name}' was deleted before completion.")]
    JobDeletedBeforeCompletion { name: String } = 5002,
    #[error("Regular pod '{name}' was deleted before becoming ready.")]
    PodDeletedBeforeReady { name: String } = 5003,
    #[error("Regular deployment '{name}' failed.")]
    DeploymentFailed { name: String } = 5016,
    #[error("Regular deployment from '{key}' was created without a metadata.name.")]
    DeploymentMissingName { key: String } = 5017,
    #[error("Regular deployment '{name}' was deleted before becoming available.")]
    DeploymentDeletedBeforeAvailable { name: String } = 5018,
    #[error("Regular cronjob from '{key}' was created without a metadata.name.")]
    CronJobMissingName { key: String } = 5010,
    #[error("Regular cronjob '{name}' was deleted before being scheduled.")]
    CronJobDeletedBeforeScheduled { name: String } = 5011,
    #[error(transparent)]
    Kubernetes(#[from] KubernetesError) = 5004,
    #[error("Could not parse config '{config}' while preparing secret env injection: {cause}.")]
    SecretEnvInjectionConfigParseFailed { config: String, cause: String } = 5005,
    #[error("Could not parse config '{config}' while sizing pod '{pod}': {cause}.")]
    SizingConfigParseFailed {
        config: String,
        pod: String,
        cause: String,
    } = 5019,
    #[error(
        "Could not extract secret leaves from config '{config}' while preparing secret env injection: {cause}."
    )]
    SecretEnvInjectionLeafExtractionFailed { config: String, cause: String } = 5006,
    #[error(
        "Cannot apply pod '{pod_template_key}': required Secret(s) not present in the cluster: {secrets}. \
         Run `tdkserver server start` (which auto-fills generators) or `tdkserver secret put` to provision them first."
    )]
    SecretEnvInjectionMissingSecrets {
        pod_template_key: String,
        secrets: String,
    } = 5007,
    #[error(
        "The init jobs '{first}' and '{second}' declare the same sequence {sequence}; \
         every init job must declare a unique '{label}' label."
    )]
    DuplicateInitSequence {
        first: String,
        second: String,
        sequence: i64,
        label: String,
    } = 5012,
    #[error(
        "The init job '{key}' declares an invalid sequence '{value}' in its '{label}' label; \
         the sequence must be a whole number."
    )]
    InvalidInitSequence {
        key: String,
        value: String,
        label: String,
    } = 5013,
    #[error("The init job '{key}' declares no '{label}' label.")]
    MissingInitSequence { key: String, label: String } = 5014,
    #[error(
        "Workload '{name}' has the kind '{kind}', which the regular phase does not await; \
         services are applied by the endpoint phase."
    )]
    UnawaitableWorkloadKind { name: String, kind: String } = 5015,
}
