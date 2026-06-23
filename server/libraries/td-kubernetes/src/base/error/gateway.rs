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
    #[error("Unknown placeholder kind '{kind}' in '{path}'. Supported kinds: {supported}.")]
    UnknownPlaceholderKind {
        path: String,
        kind: String,
        supported: String,
    } = 5011,
    #[error("Placeholder kind '{kind}' is not yet supported (in '{path}').")]
    UnsupportedPlaceholderKind { path: String, kind: String } = 5012,
    #[error("A list at '{path}' mixes placeholder and non-mapping items (item {item_index}).")]
    SequenceMixedItems { path: String, item_index: usize } = 5013,
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
    SequenceFirstValueNonScalar { path: String, item_index: usize } = 5017,
    #[error("A list item's identity value at '{path}' is itself a placeholder.")]
    PlaceholderAsIdentity { path: String } = 5018,
    #[error("Could not wipe the stage folder '{path}': {cause}.")]
    StageWipeFailed { path: String, cause: String } = 5019,
    #[error("Could not read the initialization folder '{path}': {cause}.")]
    InitializationReadFailed { path: String, cause: String } = 5020,
    #[error(
        "The initialization folder '{family}' contains the subfolder '{subfolder}', but it must \
         be flat; the initialization folder has been tampered with."
    )]
    InitializationNotFlat { family: String, subfolder: String } = 5021,
    #[error(
        "The initialization folder '{family}' has an unexpected file '{file}' that is not part of \
         the templates; the initialization folder has been tampered with."
    )]
    InitializationExtraFile { family: String, file: String } = 5022,
    #[error(
        "The initialization folder '{family}' is missing the template file '{file}'; the \
         initialization folder has been tampered with."
    )]
    InitializationMissingFile { family: String, file: String } = 5023,
    #[error(
        "The pod template '{file}' was modified, but pod templates cannot be customized; the \
         initialization folder has been tampered with."
    )]
    InitializationPodModified { file: String } = 5024,
    #[error("The placeholder kind '{kind}' at '{path}' requires a category ('${{{kind}:...}}').")]
    MissingPlaceholderCategory { path: String, kind: String } = 5025,
    #[error("A value is required for placeholder '${{{kind}:{category}}}' at '{path}'.")]
    MissingRequiredValue {
        path: String,
        kind: String,
        category: String,
    } = 5026,
    #[error(
        "The value at '{path}' must keep the system-controlled placeholder '{expected}' \
         unchanged, but it was set to '{found}'; the initialization folder has been tampered with."
    )]
    NonEditablePlaceholderModified {
        path: String,
        expected: String,
        found: String,
    } = 5027,
    #[error("Could not encode the secret key at '{path}': {cause}.")]
    SecretKeyEncodingFailed { path: String, cause: String } = 5028,
    #[error(
        "The structure at '{path}' differs between the template and the initialization folder."
    )]
    StructureMismatch { path: String } = 5029,
    #[error(
        "The placeholder '{identity}' at '{path}' resolves to '{second}', but elsewhere it \
         resolves to '{first}'; every occurrence must resolve to the same value."
    )]
    DivergingPlaceholderValue {
        identity: String,
        path: String,
        first: String,
        second: String,
    } = 5030,
    #[error(
        "The placeholder '{token}' at '{path}' cannot be embedded in surrounding text or share a \
         value with other placeholders; it must be the whole value."
    )]
    EmbeddedRetainedPlaceholder { path: String, token: String } = 5031,
    #[error("Could not write the staged file '{path}': {cause}.")]
    StageWriteFailed { path: String, cause: String } = 5032,
}
