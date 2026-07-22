//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::constants::{SECRET_FAMILY_CONFIG, SECRET_FAMILY_LABEL};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::CommandResult;
use crate::kube::api::{LabelFilter, list_secrets};
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct ListArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,
}

pub async fn command_list(arguments: ListArguments, globals: &GlobalArguments) -> CommandResult {
    let ListArguments { cluster, instance } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    let context = provider.kube_context().await?;
    let filter = LabelFilter::new(SECRET_FAMILY_LABEL, SECRET_FAMILY_CONFIG);
    let secrets = list_secrets(&context, Some(filter)).await?;

    for (name, keys) in secrets {
        info!("{name}");
        for key in keys {
            info!("  {key}");
        }
    }

    Ok(())
}
