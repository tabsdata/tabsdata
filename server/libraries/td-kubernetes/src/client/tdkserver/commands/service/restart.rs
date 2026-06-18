//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::{Provider, provider_for_or_tdkcontext};

#[derive(Debug, Clone, Args)]
pub struct RestartArguments {
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
        long_help = "Show the summary of resolved values before restarting the instance. \
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

pub async fn command_restart(
    arguments: RestartArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let RestartArguments {
        cluster,
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for_or_tdkcontext(cluster, instance, globals.verbose).await?;
    provider.ensure_startable_service().await?;

    let not_dead = !provider.service_dead().await;
    if not_dead {
        provider.ensure_stoppable_service().await?;
    }

    if preview {
        preview::restart_service(&provider.summary());
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the restart?") {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    restart_service(provider.as_ref(), &reporter, not_dead).await
}

pub async fn restart_service(
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    not_dead: bool,
) -> CommandResult {
    if not_dead {
        let outcome = provider.stop_service(reporter).await?;
        if matches!(outcome.cluster, ClusterStopOutcome::AlreadyStopped)
            && matches!(outcome.instance, InstanceStopOutcome::AlreadyStopped)
        {
            cwarn!("Instance was already stopped.");
        } else {
            report::service::stopped_service(&provider.summary());
        }
    } else {
        cwarn!("Instance was already stopped; starting it.");
    }

    provider.start_service(reporter).await?;
    report::service::restarted_service(&provider.summary());

    Ok(())
}
