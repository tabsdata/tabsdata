//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum NamingError {
    #[error("Identifier '{0}' cannot be converted to a kube identifier: {1}")]
    InvalidIdentifier(String, String) = 0,
    #[error("Value '{0}' is not a valid kube identifier: {1}")]
    InvalidKubeIdentifier(String, String) = 1,
    #[error("Kube identifier '{0}' does not start with the expected prefix '{1}'")]
    MissingPrefix(String, String) = 2,
}
