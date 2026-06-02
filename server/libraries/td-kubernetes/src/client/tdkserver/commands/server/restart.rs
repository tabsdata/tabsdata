//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::core::system::exit::Result;
use crate::cwarn;
use crate::runtime::provider::{Provider, provider_for_or_tdkcontext};

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
        long_help = "Show the summary of resolved values and prompt for confirmation \
                     before restarting the instance. Defaults to 'true'. \
                     Pass '--preview false' to skip the summary and prompt."
    )]
    pub preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt when --preview is 'true'. \
                     Has no effect when --preview is 'false'."
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
    provider.ensure_startable_runtime().await?;

    let alive = provider.runtime_alive().await;
    if alive {
        provider.ensure_stoppable_runtime().await?;
    }

    if preview {
        provider.preview_restart_runtime_summary();
        if !yes && !prompt_yes_no("Proceed with the restart?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    restart_runtime(provider.as_ref(), alive).await
}

pub async fn restart_runtime(provider: &dyn Provider, alive: bool) -> Result {
    if alive {
        if provider.stop_runtime().await? {
            provider.report_stopped_runtime();
        } else {
            cwarn!("Nothing to stop.");
        }
    } else {
        cwarn!("Instance is already stopped; starting it.");
    }

    provider.start_runtime().await?;
    provider.report_started_runtime();

    Ok(())
}
