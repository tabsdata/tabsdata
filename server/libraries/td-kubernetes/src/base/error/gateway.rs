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
    #[error("The create-time resolver is not yet implemented.")]
    ResolveNotYetImplemented = 5011,
}
