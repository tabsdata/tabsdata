//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::base::constants::LogLevel;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    instance_label, instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::{Provider, provider_for_instance};

#[derive(Debug, Clone, Args)]
pub struct RestartArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to restart. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "loglevel",
        value_enum,
        required = false,
        display_order = 20,
        long_help = "Log level for the kminus orchestrator and pod runner. When given, \
                     it is applied and saved as the instance's new log level; otherwise \
                     the saved value is used. Only valid with the 'kminus' provider.\n\n\
                     Example:\n  \
                     tdk restart --loglevel info"
    )]
    pub loglevel: Option<LogLevel>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before restarting the instance. \
                     Defaults to 'true'. Pass '--details false' to skip the summary."
    )]
    pub details: Option<bool>,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 120,
        long_help = "Skip the interactive confirmation prompt and proceed immediately."
    )]
    pub yes: Option<bool>,
}

pub async fn command_restart(
    arguments: RestartArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let RestartArguments {
        instance,
        loglevel,
        details,
        yes,
    } = arguments;

    let pins = PinContext::pins();
    let instance = resolved_instance(instance, &pins);
    let details = resolved_details(details, &pins);
    let yes = resolved_yes(yes, &pins);
    let verbose = globals.verbose;

    let provider = provider_for_instance(instance.value, verbose, loglevel).await?;
    provider.ensure_startable_service().await?;

    let not_dead = !provider.service_dead().await;
    if not_dead {
        provider.ensure_stoppable_service().await?;
    }

    if details.value {
        let markers = instance_markers(&provider.summary(), instance.pinned);
        preview::restart_service(&provider.summary(), &markers);
    }
    if !yes.value && !prompt_yes_no("Proceed with the restart?") {
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
            cwarn!(
                "Instance '{}' was already stopped.",
                instance_label(&provider.summary())
            );
        } else {
            report::component::service::stopped_service(&provider.summary());
        }
    } else {
        cwarn!(
            "Instance '{}' was already stopped; starting it.",
            instance_label(&provider.summary())
        );
    }

    provider.start_service(reporter).await?;
    report::component::service::restarted_service(&provider.summary());

    Ok(())
}
