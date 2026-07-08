//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

use crate::base::error::kubernetes::KubernetesError;
use crate::base::error::naming::NamingError;
use crate::base::error::profile::ProfileError;
use crate::base::error::provider::ProviderError;

#[td_error]
pub enum SecretError {
    #[error(
        "Inconsistent kminus instance state: the cluster is running but the staging folder '{staging_path}' still exists. \
         Expected exactly one of (a) cluster stopped with stage present (never started), or (b) cluster started with stage consumed. \
         Possible causes: an interrupted first-start, or a manual recreation of the stage folder."
    )]
    KminusInconsistentState { staging_path: String } = 2,
    #[error(
        "Empty path segment is not a valid env-var name: config '{config}' has a '${{secret}}' value at path '{path}' where one segment is empty. \
         Rename the offending YAML key to a non-empty identifier."
    )]
    EmptySegment { config: String, path: String } = 3,
    #[error(
        "Encoded secret keys collide in config '{config}': YAML paths '{first}' and '{second}' both produce env-var name '{key}'. \
         Rename one of the YAML keys so the two paths differ by more than a non-alphanumeric character."
    )]
    KeyCollision {
        config: String,
        key: String,
        first: String,
        second: String,
    } = 4,
    #[error(
        "Sequence at '{path}' in config '{config}' cannot be promoted to a map: its first-entry value is the '${{secret}}' placeholder. \
         The promoted key field must be a real identifier (typically an 'id'-like scalar), not a secret placeholder."
    )]
    PromotedKeyIsSecret { config: String, path: String } = 5,
    #[error(
        "Sequence at '{path}' in config '{config}' cannot be promoted to a map: item at index {item_index} is not a YAML mapping. \
         All items must be mappings sharing the same first key so the list can be addressed by that key."
    )]
    SequenceMixedItems {
        config: String,
        path: String,
        item_index: usize,
    } = 6,
    #[error(
        "Sequence at '{path}' in config '{config}' cannot be promoted to a map: item at index {item_index} starts with key '{other_key}' \
         but earlier items start with '{first_key}'. All items must share the same first key."
    )]
    SequenceFirstKeyMismatch {
        config: String,
        path: String,
        first_key: String,
        item_index: usize,
        other_key: String,
    } = 7,
    #[error(
        "Sequence at '{path}' in config '{config}' cannot be promoted to a map: items at indices {first_index} and {second_index} share \
         the same first-entry value '{value}'. The promoted key must be unique across the sequence."
    )]
    SequenceDuplicateFirstValues {
        config: String,
        path: String,
        value: String,
        first_index: usize,
        second_index: usize,
    } = 8,
    #[error(
        "Sequence at '{path}' in config '{config}' cannot be promoted to a map: item at index {item_index} has a non-scalar value for its \
         first key. The promoted key field must be a scalar string."
    )]
    SequenceFirstValueNonScalar {
        config: String,
        path: String,
        item_index: usize,
    } = 9,
    #[error(
        "Sequence at '{path}' in config '{config}' cannot be promoted to a map: item at index {item_index} has no entries. \
         Each item must contain at least the key field followed by its data."
    )]
    SequenceEmptyItem {
        config: String,
        path: String,
        item_index: usize,
    } = 10,
    #[error("Could not serialize Secret manifest to YAML: {0}.")]
    ManifestSerializeFailed(String) = 5000,
    #[error("Could not read staged file '{path}': {cause}.")]
    StageReadFailed { path: String, cause: String } = 5001,
    #[error("Could not read the current source from the cluster: {cause}.")]
    CurrentSourceReadFailed { cause: String } = 5002,
    #[error("Configmap '{configmap}' entry value is not valid UTF-8: {cause}.")]
    ConfigmapEntryNotUtf8 { configmap: String, cause: String } = 5003,
    #[error("Could not parse config '{config}' as YAML: {cause}.")]
    ConfigYamlParseFailed { config: String, cause: String } = 5007,
    #[error("Could not parse secret manifest '{file}': {cause}.")]
    ManifestParseFailed { file: String, cause: String } = 5008,
    #[error(
        "Secret manifest '{file}' has no 'metadata.name'. \
         Add a name so the manifest identifies which secret to update."
    )]
    ManifestMissingName { file: String } = 5009,
    #[error(
        "Config '{config}' has no matching secret '{secret}'. Every config requires its \
         secret, even when it declares no secret keys.\n\
         Expected keys:{expected}"
    )]
    SettlementSecretMissing {
        config: String,
        secret: String,
        expected: String,
    } = 5011,
    #[error(
        "Secret '{secret}' keys do not match those declared by config '{config}'.\n\
         Extra keys:{extra}\n\
         Missing keys:{missing}"
    )]
    SettlementKeysMismatch {
        config: String,
        secret: String,
        extra: String,
        missing: String,
    } = 5013,
    #[error("Cannot start the instance: configs and secrets are not in sync.{failures}")]
    SettlementBatchFailed { failures: String } = 5014,
    #[error(
        "Config '{config}' uses unknown secret generator '${{secretAuto:{name}}}'. \
         Supported generators: {supported}."
    )]
    UnknownGeneratorName {
        config: String,
        name: String,
        supported: String,
    } = 5015,
    #[error("Could not sign the '{generator}' service token: {cause}.")]
    TokenSigningFailed { generator: String, cause: String } = 5016,
    #[error(
        "Cannot derive the '{generator}' secret: its dependency '{dependency}' is not declared by \
         any config (no '${{secretAuto:{dependency}}}' placeholder exists to generate it)."
    )]
    SecretDependencyMissing {
        generator: String,
        dependency: String,
    } = 5017,
    #[error(
        "Cannot derive the '{generator}' secret: its dependency chain is cyclic \
         (it transitively depends on itself). Fix the generator dependency declarations \
         so the dependency graph is acyclic."
    )]
    SecretDependencyCyclic { generator: String } = 5018,
    #[error(
        "Generated secret '{generator}' has divergent values across configs: two configs \
         declare '${{secretAuto:{generator}}}' but their stored secret values differ. \
         A generated secret must have a single value across the instance."
    )]
    GeneratorValueDivergent { generator: String } = 5019,
    #[error(transparent)]
    Profile(#[from] ProfileError) = 5004,
    #[error(transparent)]
    Kubernetes(#[from] KubernetesError) = 5005,
    #[error(transparent)]
    Naming(#[from] NamingError) = 5006,
    #[error(transparent)]
    Provider(#[from] ProviderError) = 5010,
}
