//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;

use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::core::system::exit::{Result, abort};
use crate::cwarn;
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct StartArguments {
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
        long_help = "Show the summary of resolved values before starting the instance. \
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

pub async fn command_start(arguments: StartArguments, globals: &GlobalArguments) -> Result {
    let StartArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_startable_runtime().await?;

    if provider.runtime_alive().await {
        cwarn!("Instance is already started.");
        return Ok(());
    }

    if preview {
        provider.preview_start_runtime_summary();
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the start?") {
        return abort(Cancelled);
    }

    provider.start_runtime().await?;
    provider.report_started_runtime();

    Ok(())
}
