//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeClientError {
    #[error("Could not build the Kubernetes client: {0}")]
    BuildFailed(String) = 5000,
    #[error("Could not serialize the active context: {0}")]
    ContextSerializeFailed(String) = 5001,
}
