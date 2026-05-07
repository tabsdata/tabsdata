//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::cwarn;
use crate::fabric::fabric_for_or_tdkcontext;
use crate::system::exit::Outcome;

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct StopArguments {
    #[command(flatten)]
    cluster: ClusterArguments,

    #[command(flatten)]
    instance: InstanceArguments,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values and prompt for confirmation \
                     before stopping the instance. Defaults to 'true'. \
                     Pass '--preview false' to skip the summary and prompt."
    )]
    preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately. \
                     Has no effect when --preview is 'false'."
    )]
    yes: bool,
}

pub async fn command_stop(arguments: StopArguments) -> Outcome {
    let StopArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let fabric = fabric_for_or_tdkcontext(cluster, instance).await?;
    fabric.ensure_stoppable_runtime().await?;

    if preview {
        fabric.preview_stop_runtime_summary();
        if !yes && !prompt_yes_no("Proceed with the stop?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    if fabric.stop_runtime().await? {
        fabric.report_stopped_runtime();
    } else {
        cwarn!("Nothing to stop.");
    }

    Ok(())
}
