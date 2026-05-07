//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::fabric::{Fabric, fabric_for_or_tdkcontext};
use crate::system::exit::Result;

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
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values and prompt for confirmation \
                     before starting. Defaults to 'false'. \
                     Pass '--preview' or '--preview true' to opt in."
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

pub async fn command_start(arguments: StartArguments) -> Result {
    let StartArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let fabric = fabric_for_or_tdkcontext(cluster, instance).await?;
    fabric.ensure_startable_runtime().await?;

    if preview {
        fabric.preview_start_runtime_summary();
        if !yes && !prompt_yes_no("Proceed with the start?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    fabric.start_runtime().await?;
    fabric.report_started_runtime();

    Ok(())
}
