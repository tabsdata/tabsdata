//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeconfigError {
    #[error("Could not load Kubernetes configuration: {0}.")]
    LoadFailed(String) = 5000,
    #[error("Could not read kubeconfig at '{0}': {1}.")]
    ReadFailed(String, String) = 5001,
    #[error("Could not parse kubeconfig: {0}.")]
    ParseFailed(String) = 5002,
    #[error("Could not write kubeconfig at '{0}': {1}.")]
    WriteFailed(String, String) = 5003,
    #[error("Could not delete kubeconfig at '{0}': {1}.")]
    DeleteFailed(String, String) = 5004,
}
