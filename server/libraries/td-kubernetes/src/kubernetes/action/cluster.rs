//
// Copyright 2026 Tabs Data Inc.
//

use kube::Client;
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::exit::{Outcome, abort};

pub async fn ensure_cluster_reachable(client: &Client) -> Outcome {
    if let Err(error) = client.apiserver_version().await {
        cerror!("The kubernetes cluster is not reachable: {error}");
        return abort(GeneralError);
    }
    Ok(())
}
