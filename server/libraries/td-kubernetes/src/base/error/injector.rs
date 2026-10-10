//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum InjectorError {
    #[error("Setting {origin} is not valid: {reason}.")]
    InvalidSetting { origin: String, reason: String } = 0,
    #[error("Settings file '{file}' is not valid: {reason}.")]
    InvalidSettingsFile { file: String, reason: String } = 1,
    #[error(
        "Setting {origin} reads environment variable '{variable}', which is not set on this \
         machine."
    )]
    EnvironmentVariableMissing { origin: String, variable: String } = 2,
    #[error(
        "Setting {origin} names placeholder category '{category}' with {count} kinds \
         ({candidates}); qualify it as one of them."
    )]
    AmbiguousCategory {
        origin: String,
        category: String,
        count: usize,
        candidates: String,
    } = 3,
    #[error(
        "Setting {origin} targets placeholder '{identity}', whose kind '{kind}' is \
         system-controlled and cannot be set."
    )]
    NonEditablePlaceholder {
        origin: String,
        identity: String,
        kind: String,
    } = 4,
    #[error("Setting {origin} targets placeholder '{identity}', whose kind is not supported yet.")]
    UnsupportedPlaceholder { origin: String, identity: String } = 5,
    #[error(
        "Setting {origin} targets '{address}', which holds placeholder '{identity}' shared by \
         every file that uses it; set it as '{identity}' instead."
    )]
    PlaceholderAtKey {
        origin: String,
        address: String,
        identity: String,
    } = 6,
    #[error(
        "Setting {origin} targets '{address}', which holds a system-controlled placeholder and \
         cannot be set."
    )]
    NonEditableKey { origin: String, address: String } = 7,
    #[error("Setting {origin} targets '{address}', which is not a final node: it has subnodes.")]
    NotALeaf { origin: String, address: String } = 8,
    #[error("Setting {origin} targets '{address}', whose current value is {expected}; {reason}.")]
    ValueTypeMismatch {
        origin: String,
        address: String,
        expected: String,
        reason: String,
    } = 9,
    #[error("Setting {origin} has a value that is not a single scalar.")]
    ValueNotScalar { origin: String } = 10,
    #[error("Setting {origin} gives placeholder '{identity}' an empty value, but it is mandatory.")]
    EmptyMandatoryValue { origin: String, identity: String } = 11,
    #[error(
        "Settings {first} and {second} both set '{target}'; a value can be specified only once."
    )]
    DuplicateSetting {
        first: String,
        second: String,
        target: String,
    } = 12,
    #[error("Setting {origin} targets '{address}', which cannot be edited in place: {reason}.")]
    UnsupportedLayout {
        origin: String,
        address: String,
        reason: String,
    } = 13,
    #[error(
        "Setting {origin} names file '{file}', which is not an editable config file; \
             editable files are: {available}."
    )]
    UnknownFile {
        origin: String,
        file: String,
        available: String,
    } = 1000,
    #[error(
        "Setting {origin} names placeholder category '{category}', which no config file \
             uses.{suggestion}"
    )]
    UnknownCategory {
        origin: String,
        category: String,
        suggestion: String,
    } = 1001,
    #[error(
        "Setting {origin} targets '{address}', which does not exist; settings can change \
         existing values only, not add new ones."
    )]
    KeyNotFound { origin: String, address: String } = 1002,
    #[error(
        "Setting {origin} targets '{address}', where selector '{selector}' matches {count} \
             list items; it must match exactly one."
    )]
    SelectorMismatch {
        origin: String,
        address: String,
        selector: String,
        count: usize,
    } = 1003,
    #[error("Could not read settings file '{file}': {cause}.")]
    SettingsFileReadFailed { file: String, cause: String } = 5000,
    #[error("Could not parse config file '{file}': {cause}.")]
    ConfigParseFailed { file: String, cause: String } = 5001,
    #[error(
        "Injecting the settings into config file '{file}' did not produce the expected \
             content: {cause}."
    )]
    InjectionVerifyFailed { file: String, cause: String } = 5002,
}
