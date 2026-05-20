//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum ResolverError {
    #[error("Could not read configuration file '{path}': {cause}")]
    ReadFailed { path: String, cause: String } = 5000,
    #[error("Could not parse configuration file '{path}' as YAML: {cause}")]
    ParseFailed { path: String, cause: String } = 5001,
    #[error("Environment variable '{variable}' not found")]
    EnvironmentVariableNotFound { variable: String } = 5003,
    #[error("Invalid Hashicorp secret spec '{spec}'. Expected format: 'path;name'")]
    InvalidHashicorpSpec { spec: String } = 5004,
    #[error("Hashicorp Vault URL and token must be provided")]
    MissingHashicorpCredentials = 5005,
    #[error("Could not read Hashicorp secret at path '{path}', name '{name}': {cause}")]
    HashicorpReadFailed {
        path: String,
        name: String,
        cause: String,
    } = 5006,
    #[error("Secret key '{name}' not found at Vault path '{path}'")]
    HashicorpKeyNotFound { path: String, name: String } = 5007,
    #[error("Unsupported system key '{key}'. Supported keys: {supported}")]
    UnsupportedSystemKey { key: String, supported: String } = 5008,
    #[error("Resolver source '{name}' is declared but has no implementation yet")]
    UnimplementedSource { name: String } = 5009,
    #[error("System value '{key}' was not provided to the resolver")]
    MissingSystemValue { key: String } = 5010,
}
