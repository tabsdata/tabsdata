//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;

use crate::base::reporter::InteractiveReporter;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort};
use crate::cwarn;
use crate::runtime::provider::provider_for_instance;

#[derive(Debug, Clone, Args)]
pub struct StartArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Name of the instance to start. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before starting the instance. \
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

pub async fn command_start(arguments: StartArguments, globals: &GlobalArguments) -> CommandResult {
    let StartArguments {
        instance,
        preview,
        yes,
    } = arguments;

    let provider = provider_for_instance(instance, globals.verbose).await?;
    provider.ensure_startable_service().await?;

    if provider.service_alive().await {
        cwarn!("Instance is already started.");
        return Ok(());
    }

    if preview {
        preview::start_service(&provider.summary());
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the start?") {
        return abort(Cancelled);
    }

    let reporter = InteractiveReporter::new();
    provider.start_service(&reporter).await?;
    report::service::started_service(&provider.summary());

    Ok(())
}
