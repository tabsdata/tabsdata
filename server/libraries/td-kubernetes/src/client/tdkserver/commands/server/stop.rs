//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::reporter::InteractiveReporter;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct StopArguments {
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
        long_help = "Show the summary of resolved values before stopping the instance. \
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

pub async fn command_stop(arguments: StopArguments, globals: &GlobalArguments) -> CommandResult {
    let StopArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_stoppable_server().await?;

    if provider.server_dead().await {
        cwarn!("Server is already stopped.");
        return Ok(());
    }

    if preview {
        preview::stop_server(&provider.summary());
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the stop?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    let outcome = provider.stop_server(&reporter).await?;
    if matches!(outcome.cluster, ClusterStopOutcome::AlreadyStopped)
        && matches!(outcome.instance, InstanceStopOutcome::AlreadyStopped)
    {
        cwarn!("Server is already stopped.");
    } else {
        report::stopped_server(&provider.summary());
    }

    Ok(())
}
