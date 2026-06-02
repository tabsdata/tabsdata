//
// Copyright 2026 Tabs Data Inc.
//

use std::path::PathBuf;

use clap_derive::Args;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::config::put::{ensure_source_folder, put_configs};
use crate::client::tdkserver::commands::server::restart::restart_runtime;
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::core::system::exit::Result;
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct ApplyArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "folder",
        required = true,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Source folder. Puts the config key values from the folder, then \
                     restarts the instance so they take effect. Restart happens only \
                     if the put succeeds."
    )]
    pub folder: PathBuf,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before applying. \
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

pub async fn command_apply(arguments: ApplyArguments, globals: &GlobalArguments) -> Result {
    let ApplyArguments {
        cluster,
        instance,
        folder,
        preview,
        yes,
    } = arguments;

    ensure_source_folder(&folder)?;
    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_startable_runtime().await?;

    let alive = provider.runtime_alive().await;
    if alive {
        provider.ensure_stoppable_runtime().await?;
    }

    if preview {
        provider.preview_apply_runtime_summary();
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the apply?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    put_configs(provider.as_ref(), &folder).await?;
    restart_runtime(provider.as_ref(), alive).await
}
