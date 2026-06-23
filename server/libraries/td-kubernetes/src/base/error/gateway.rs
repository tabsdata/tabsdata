//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum ResolverError {
    #[error("Could not read configuration file '{path}': {cause}.")]
    ReadFailed { path: String, cause: String } = 5000,
    #[error("Could not parse configuration file '{path}' as YAML: {cause}.")]
    ParseFailed { path: String, cause: String } = 5001,
    #[error("Environment variable '{variable}' not found.")]
    EnvironmentVariableNotFound { variable: String } = 5003,
    #[error("Unsupported system key '{key}'. Supported keys: {supported}.")]
    UnsupportedSystemKey { key: String, supported: String } = 5008,
    #[error("System value '{key}' was not provided to the resolver.")]
    MissingSystemValue { key: String } = 5010,
    #[error(
        "Unknown placeholder kind '{kind}' in '{path}'. Supported kinds: {supported}."
    )]
    UnknownPlaceholderKind {
        path: String,
        kind: String,
        supported: String,
    } = 5011,
    #[error("Placeholder kind '{kind}' is not yet supported (in '{path}').")]
    UnsupportedPlaceholderKind { path: String, kind: String } = 5012,
    #[error(
        "A list at '{path}' mixes placeholder and non-mapping items (item {item_index})."
    )]
    SequenceMixedItems {
        path: String,
        item_index: usize,
    } = 5013,
    #[error("A list item at '{path}' (item {item_index}) is an empty mapping.")]
    SequenceEmptyItem { path: String, item_index: usize } = 5014,
    #[error(
        "A list at '{path}' has items with differing first keys ('{first_key}' vs \
         '{other_key}' at item {item_index}); it cannot carry placeholders."
    )]
    SequenceFirstKeyMismatch {
        path: String,
        first_key: String,
        item_index: usize,
        other_key: String,
    } = 5015,
    #[error(
        "A list at '{path}' has a duplicated first value '{value}' (items \
         {first_index} and {second_index}); it cannot carry placeholders."
    )]
    SequenceDuplicateFirstValues {
        path: String,
        value: String,
        first_index: usize,
        second_index: usize,
    } = 5016,
    #[error("A list item's identity key at '{path}' (item {item_index}) is not a scalar.")]
    SequenceFirstValueNonScalar {
        path: String,
        item_index: usize,
    } = 5017,
    #[error("A list item's identity value at '{path}' is itself a placeholder.")]
    PlaceholderAsIdentity { path: String } = 5018,
}
