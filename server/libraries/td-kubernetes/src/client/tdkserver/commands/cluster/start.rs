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
use crate::cwarn;
use crate::runtime::provider::outcome::ClusterStartOutcome;
use crate::runtime::provider::provider_for_or_tdkcontext;

#[derive(Debug, Clone, Args)]
pub struct StartArguments {
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
        long_help = "Show the summary of resolved values before starting the cluster. \
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

pub async fn command_start(arguments: StartArguments, globals: &GlobalArguments) -> CommandResult {
    let StartArguments {
        cluster,
        instance,
        details,
        yes,
    } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_startable_cluster().await?;

    if provider.cluster_alive().await {
        cwarn!("Cluster is already started.");
        return Ok(());
    }

    if details {
        preview::start_cluster(&provider.view());
    }
    if !confirm(yes, "Proceed with the start?")? {
        return abort(Cancelled);
    }

    let reporter = InteractiveReporter::new();
    match provider.start_cluster(&reporter).await? {
        ClusterStartOutcome::Started => {
            report::component::cluster::started_cluster(&provider.view())
        }
        ClusterStartOutcome::AlreadyRunning => cwarn!("Cluster is already started."),
        ClusterStartOutcome::NotSupported => cwarn!(
            "The cluster lifecycle is not managed by tabsdata for the '{}' provider.",
            provider.cluster_provider().as_str()
        ),
    }

    Ok(())
}
