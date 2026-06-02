//
// Copyright 2026 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeNamespaceError {
    #[error("Namespace '{0}' does not exist in the cluster.")]
    NotFound(String) = 1000,
    #[error("Could not verify namespace '{0}': {1}.")]
    QueryFailed(String, String) = 5000,
}
