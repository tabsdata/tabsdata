//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum KubeClusterError {
    #[error("Kubernetes cluster is not reachable: {0}.")]
    Unreachable(String) = 5000,
}
