//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::reporter::InteractiveReporter;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::render::{preview, report};
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::core::system::exit::Result;
use crate::cwarn;
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct RestartArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before restarting the cluster. \
                     Defaults to 'true'. Pass '--preview false' to skip the summary."
    )]
    pub preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately."
    )]
    pub yes: bool,
}

pub async fn command_restart(arguments: RestartArguments, globals: &GlobalArguments) -> Result {
    let RestartArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_restartable_cluster().await?;

    if provider.cluster_dead().await {
        cwarn!("Cluster was already stopped; starting it.");
    }

    if preview {
        preview::restart_cluster(&provider.summary());
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the restart?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    provider.restart_cluster(&reporter).await?;
    report::restarted_cluster(&provider.summary());

    Ok(())
}
