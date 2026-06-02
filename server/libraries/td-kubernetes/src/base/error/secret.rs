//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum SecretError {
    #[error(
        "Reading the current source requires a started cluster. \
         Start the cluster with `tdkserver server start ...` or use `--source default` to read from the kprofile."
    )]
    GetCurrentRequiresStartedCluster = 1,
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
}
