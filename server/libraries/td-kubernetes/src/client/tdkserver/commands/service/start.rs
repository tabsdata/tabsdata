//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;

use crate::base::constants::LogLevel;
use crate::base::reporter::InteractiveReporter;
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    instance_label, instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort};
use crate::cwarn;
use crate::runtime::provider::provider_for_instance_mutating;

#[derive(Debug, Clone, Args)]
pub struct StartArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to start. Defaults to 'tabsdata'."
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
                     tdk start --loglevel info"
    )]
    pub loglevel: Option<LogLevel>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before starting the instance. \
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

pub async fn command_start(arguments: StartArguments, globals: &GlobalArguments) -> CommandResult {
    let StartArguments {
        instance,
        loglevel,
        details,
        yes,
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let verbose = globals.verbose;

    let provider = provider_for_instance_mutating(instance.value, verbose, loglevel).await?;
    provider.ensure_startable_service().await?;

    if provider.service_alive().await {
        cwarn!(
            "Instance '{}' is already started.",
            instance_label(&provider.view())
        );
        return Ok(());
    }

    if details.value {
        let markers = instance_markers(&provider.view(), instance.in_settings);
        preview::start_service(&provider.view(), &markers);
    }
    if !confirm(yes.value, "Proceed with the start?")? {
        return abort(Cancelled);
    }

    let reporter = InteractiveReporter::new();
    provider.start_service(&reporter).await?;
    report::component::service::started_service(&provider.view());

    Ok(())
}
