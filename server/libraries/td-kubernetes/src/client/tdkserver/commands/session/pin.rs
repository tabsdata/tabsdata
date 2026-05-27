//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::core::system::exit::Result;
use crate::runtime::provider::provider_for;

#[derive(Debug, Clone, Args)]
pub struct PinArguments {
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
                     before storing the active tabsdata session. Defaults to 'true'. \
                     Pass '--preview false' to skip the summary and prompt."
    )]
    pub preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately. \
                     Has no effect when --preview is 'false'."
    )]
    pub yes: bool,
}

pub async fn command_pin(arguments: PinArguments, globals: &GlobalArguments) -> Result {
    let PinArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for(cluster, instance, globals.verbose).await?;
    provider.ensure_pinnable_runtime().await?;

    if preview {
        provider.preview_pin_runtime_summary();
        if !yes && !prompt_yes_no("Pin this as the active tabsdata session?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    provider.to_tdkcontext().save()?;
    provider.report_pinned_runtime();

    Ok(())
}
