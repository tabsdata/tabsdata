//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;

use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::core::system::exit::{Result, abort};
use crate::runtime::provider::provider_for;

#[derive(Debug, Clone, Args)]
pub struct CreateArguments {
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
                     before creating the instance. Defaults to 'true'. \
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

pub async fn command_create(arguments: CreateArguments, globals: &GlobalArguments) -> Result {
    let CreateArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for(cluster, instance, globals.verbose).await?;
    provider.ensure_creatable_runtime().await?;

    if preview {
        provider.preview_create_runtime_summary();
        if !yes && !prompt_yes_no("Proceed with the creation?") {
            return abort(Cancelled);
        }
    }

    let tx = provider.prepare_cluster().await?;
    provider.create_database().await?;
    provider.create_runtimes().await?;
    provider.prepare_instance().await?;
    tx.commit();
    provider.report_created_runtime();

    Ok(())
}
