//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::exit::Outcome;
use crate::fabric::fabric_for;

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct StopArguments {
    #[command(flatten)]
    cluster: ClusterArguments,

    #[command(flatten)]
    instance: InstanceArguments,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately. \
                     The summary of resolved values is always shown beforehand."
    )]
    yes: bool,
}

pub async fn command_stop(arguments: StopArguments) -> Outcome {
    let StopArguments {
        cluster,
        instance,
        yes,
    } = arguments;

    let fabric = fabric_for(cluster, instance).await?;
    fabric.ensure_stoppable().await?;
    fabric.preview_stop_summary();

    if !yes && !prompt_yes_no("Proceed with the stop?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    fabric.stop_runtime().await?;
    fabric.report_stopped();

    Ok(())
}
