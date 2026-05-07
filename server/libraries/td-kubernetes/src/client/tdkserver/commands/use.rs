//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::fabric::fabric_for;
use crate::system::exit::Outcome;

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct UseArguments {
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
                     before storing the active context. Defaults to 'true'. \
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

pub async fn command_use(arguments: UseArguments) -> Outcome {
    let UseArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let fabric = fabric_for(cluster, instance).await?;
    fabric.ensure_usable_runtime().await?;

    if preview {
        fabric.preview_use_runtime_summary();
        if !yes && !prompt_yes_no("Set this as the active context?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    fabric.to_tdkcontext().save()?;
    fabric.report_used_runtime();

    Ok(())
}
