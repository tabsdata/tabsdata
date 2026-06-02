//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::Result;
use crate::kube::api::list_configmaps;
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct ListArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,
}

pub async fn command_list(arguments: ListArguments, globals: &GlobalArguments) -> Result {
    let ListArguments { cluster, instance } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    let context = provider.kube_context().await?;
    let configs = list_configmaps(&context).await?;

    for (name, keys) in configs {
        info!("{name}");
        for key in keys {
            info!("  {key}");
        }
    }

    Ok(())
}
