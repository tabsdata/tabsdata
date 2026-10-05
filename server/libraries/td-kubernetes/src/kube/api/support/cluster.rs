//
// Copyright 2026 Tabsdata Inc.
//

use kube::Client;

use crate::base::error::kubernetes::{KubeClusterError, KubernetesError};

pub async fn ensure_cluster_reachable(client: &Client) -> Result<(), KubernetesError> {
    client
        .apiserver_version()
        .await
        .map(|_| ())
        .map_err(|error| KubeClusterError::Unreachable(error.to_string()).into())
}
