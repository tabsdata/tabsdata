//
// Copyright 2026 Tabs Data Inc.
//

use kube::Client;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::core::system::exit::{Result, abort};

pub async fn ensure_cluster_reachable(client: &Client) -> Result {
    if let Err(error) = client.apiserver_version().await {
        cerror!("The kubernetes cluster is not reachable: {error}");
        return abort(GeneralError);
    }
    Ok(())
}
