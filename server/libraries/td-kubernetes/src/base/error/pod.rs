//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum PodYamlError {
    #[error("Could not parse pod YAML: {0}")]
    ParseFailed(String) = 5000,
    #[error("Could not serialize pod YAML: {0}")]
    SerializeFailed(String) = 5001,
    #[error("Could not register pod template: {0}")]
    TemplateRegisterFailed(String) = 5002,
    #[error("Could not render pod template: {0}")]
    TemplateRenderFailed(String) = 5003,
}

#[cfg(test)]
mod tests;
