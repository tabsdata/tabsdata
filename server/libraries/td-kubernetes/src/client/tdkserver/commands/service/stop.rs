//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::reporter::InteractiveReporter;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::provider_for_instance;

#[derive(Debug, Clone, Args)]
pub struct StopArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Name of the instance to stop. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

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
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for_instance(instance, globals.verbose).await?;
    provider.ensure_stoppable_service().await?;

    if provider.service_dead().await {
        cwarn!("Instance is already stopped.");
        return Ok(());
    }

    if preview {
        preview::stop_service(&provider.summary());
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the stop?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    let outcome = provider.stop_service(&reporter).await?;
    if matches!(outcome.cluster, ClusterStopOutcome::AlreadyStopped)
        && matches!(outcome.instance, InstanceStopOutcome::AlreadyStopped)
    {
        cwarn!("Instance is already stopped.");
    } else {
        report::service::stopped_service(&provider.summary());
    }

    Ok(())
}
