//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use tracing::info;

use crate::base::reporter::InteractiveReporter;
use crate::client::registry::instance_folder;
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    instance_label, instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::provider_for_instance;

#[derive(Debug, Clone, Args)]
pub struct StopArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to stop. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before stopping the instance. \
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

pub async fn command_stop(arguments: StopArguments, globals: &GlobalArguments) -> CommandResult {
    let StopArguments {
        instance,
        details,
        yes,
    } = arguments;

    let pins = PinContext::pins();
    let instance = resolved_instance(instance, &pins);
    let details = resolved_details(details, &pins);
    let yes = resolved_yes(yes, &pins);
    let verbose = globals.verbose;

    let name = instance
        .value
        .clone()
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    if !instance_folder(&name)?.exists() {
        cwarn!("Instance '{name}' does not exist. Nothing to stop.");
        return Ok(());
    }

    let provider = provider_for_instance(instance.value, verbose, None).await?;
    provider.ensure_stoppable_service().await?;

    if provider.service_dead().await {
        cwarn!(
            "Instance '{}' is already stopped.",
            instance_label(&provider.summary())
        );
        return Ok(());
    }

    if details.value {
        let markers = instance_markers(&provider.summary(), instance.pinned);
        preview::stop_service(&provider.summary(), &markers);
    }
    if !confirm(yes.value, "Proceed with the stop?")? {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    let outcome = provider.stop_service(&reporter).await?;
    if matches!(outcome.cluster, ClusterStopOutcome::AlreadyStopped)
        && matches!(outcome.instance, InstanceStopOutcome::AlreadyStopped)
    {
        cwarn!(
            "Instance '{}' is already stopped.",
            instance_label(&provider.summary())
        );
    } else {
        report::component::service::stopped_service(&provider.summary());
    }

    Ok(())
}
