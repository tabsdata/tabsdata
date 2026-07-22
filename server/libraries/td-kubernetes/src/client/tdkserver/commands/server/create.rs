//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;

use crate::base::reporter::InteractiveReporter;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort};
use crate::runtime::provider::provider_for;

#[derive(Debug, Clone, Args)]
pub struct CreateArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[arg(
        long,
        name = "details",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before creating the instance. \
                     Defaults to 'true'. Pass '--details false' to skip the summary."
    )]
    pub details: bool,

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

pub async fn command_create(
    arguments: CreateArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let CreateArguments {
        cluster,
        instance,
        details,
        yes,
    } = arguments;

    let provider = provider_for(cluster, instance, globals.verbose, None, None).await?;
    provider.ensure_creatable_server().await?;

    if details {
        preview::create_server(&provider.summary());
    }
    if !confirm(yes, "Proceed with the creation?")? {
        return abort(Cancelled);
    }

    let reporter = InteractiveReporter::new();
    let tx = provider.create_cluster_hook(&reporter).await?;
    provider.create_database(&reporter).await?;
    provider.create_runtimes(&reporter).await?;
    provider.create_images(&reporter).await?;
    tx.commit();
    report::component::server::created_server(&provider.summary());

    Ok(())
}
