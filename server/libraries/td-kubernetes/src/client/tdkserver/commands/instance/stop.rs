//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::reporter::InteractiveReporter;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::provider::outcome::InstanceStopOutcome;
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct StopArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "details",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before stopping the instance. \
                     Defaults to 'true'. Pass '--details false' to skip the summary."
    )]
    pub details: bool,

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

pub async fn command_stop(arguments: StopArguments, globals: &GlobalArguments) -> CommandResult {
    let StopArguments {
        cluster,
        instance,
        details,
        yes,
    } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_stoppable_instance().await?;

    if provider.instance_dead().await {
        cwarn!("Instance is already stopped.");
        return Ok(());
    }

    if details {
        preview::stop_instance(&provider.summary());
    }
    if !yes && !prompt_yes_no("Proceed with the stop?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    match provider.stop_instance(&reporter).await? {
        InstanceStopOutcome::Stopped => {
            report::component::instance::stopped_instance(&provider.summary())
        }
        InstanceStopOutcome::AlreadyStopped => cwarn!("Nothing to stop."),
    }

    Ok(())
}
