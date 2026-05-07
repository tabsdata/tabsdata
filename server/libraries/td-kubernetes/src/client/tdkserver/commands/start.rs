//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::exit::Outcome;
use crate::fabric::fabric_for_or_tdkcontext;

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct StartArguments {
    #[command(flatten)]
    cluster: ClusterArguments,

    #[command(flatten)]
    instance: InstanceArguments,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values and prompt for confirmation \
                     before starting. Defaults to 'false'. \
                     Pass '--preview' or '--preview true' to opt in."
    )]
    preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt when --preview is 'true'. \
                     Has no effect when --preview is 'false'."
    )]
    yes: bool,
}

pub async fn command_start(arguments: StartArguments) -> Outcome {
    let StartArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let fabric = fabric_for_or_tdkcontext(cluster, instance).await?;
    fabric.ensure_startable().await?;

    if preview {
        fabric.preview_start_summary();
        if !yes && !prompt_yes_no("Proceed with the start?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    fabric.start_runtime().await?;
    fabric.ingest_configmap().await?;
    fabric.report_started();

    Ok(())
}
