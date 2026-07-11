//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
#[cfg(feature = "enterprise")]
use td_common::status::ExitStatus::Cancelled;
use tracing::warn;

use crate::base::constants::LogLevel;
#[cfg(feature = "enterprise")]
use crate::base::constants::{SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY};
use crate::client::registry::instance_folder;
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments};
use crate::client::tdkserver::commands::service::create::create_instance;
use crate::client::tdkserver::commands::service::init::{InitArguments, command_init};
use crate::client::tdkserver::commands::service::start::{StartArguments, command_start};
use crate::client::tdkserver::commands::service::trash::trash_instance_folder;
use crate::client::tdkserver::commands::service::{
    resolved_details, resolved_instance, resolved_yes,
};
#[cfg(feature = "enterprise")]
use crate::client::tdkserver::common::prompt_yes_no;
#[cfg(feature = "enterprise")]
use crate::core::system::exit::abort;
use crate::core::system::exit::{CommandResult, render_error};
use crate::cwarn;
use crate::runtime::instance::ports::DEFAULT_BASE_PORT;

#[derive(Debug, Clone, Args)]
pub struct QuickstartArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to initialize, create and start. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "loglevel",
        value_enum,
        required = false,
        display_order = 75,
        long_help = "Log level for the kminus orchestrator and pod runner. \
                     Defaults to 'debug'. Only valid with the 'kminus' provider.\n\n\
                     Example:\n  \
                     tdk quickstart --provider kminus --loglevel info"
    )]
    pub loglevel: Option<LogLevel>,

    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[arg(
        long,
        name = "port",
        required = false,
        default_value_t = DEFAULT_BASE_PORT,
        display_order = 70,
        long_help = "Base HTTP/HTTPS port. Ports for the instance services are assigned \
                     sequentially starting from this value."
    )]
    pub port: u16,

    #[cfg(feature = "enterprise")]
    #[arg(
        long,
        name = "openai-key",
        required = false,
        display_order = 80,
        long_help = "OpenAI API key for the aiagent. Optional."
    )]
    pub openai_key: Option<String>,

    #[cfg(feature = "enterprise")]
    #[arg(
        long,
        name = "anthropic-key",
        required = false,
        display_order = 90,
        long_help = "Anthropic API key for the aiagent. Optional."
    )]
    pub anthropic_key: Option<String>,

    #[arg(
        long,
        name = "save",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 100,
        long_help = "Copy the cluster settings files (kubeconfig, etc.) into the instance \
                     folder so the instance is self-contained."
    )]
    pub save: Option<bool>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before each step. Defaults to \
                     'true'. Pass '--details false' to skip the summaries."
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
        long_help = "Skip the interactive confirmation prompts and proceed immediately."
    )]
    pub yes: Option<bool>,
}

pub async fn command_quickstart(
    arguments: QuickstartArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let QuickstartArguments {
        instance,
        cluster,
        port,
        #[cfg(feature = "enterprise")]
        openai_key,
        #[cfg(feature = "enterprise")]
        anthropic_key,
        save,
        details,
        yes,
        loglevel,
    } = arguments;

    #[cfg_attr(not(feature = "enterprise"), allow(unused_mut))]
    let mut overrides: HashMap<String, String> = HashMap::new();
    #[cfg(feature = "enterprise")]
    {
        if let Some(value) = openai_key {
            overrides.insert(SECRET_CATEGORY_OPENAI_KEY.to_owned(), value);
        }
        if let Some(value) = anthropic_key {
            overrides.insert(SECRET_CATEGORY_ANTHROPIC_KEY.to_owned(), value);
        }
        if overrides.is_empty()
            && !prompt_yes_no("No OpenAI or Anthropic key provided. Proceed with no aiagent keys?")
        {
            return abort(Cancelled);
        }
    }

    let pins = PinContext::pins();
    let name = resolved_instance(instance.clone(), &pins)
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let folder = instance_folder(&name)?;
    let preexisting = folder.exists();

    let outcome = run_quickstart(
        instance, cluster, port, save, details, yes, loglevel, &overrides, globals,
    )
    .await;

    if outcome.is_err() && !preexisting && folder.exists() {
        match trash_instance_folder(&name, &folder) {
            Ok(destination) => {
                warn!(
                    "Warning: Command 'quickstart' failed; moved the partially created \
                     instance to trash: {}",
                    destination.display()
                );
            }
            Err(cleanup_error) => {
                let detail = match cleanup_error.source() {
                    Some(source) => render_error(source, globals.verbose),
                    None => cleanup_error.status().to_string(),
                };
                cwarn!(
                    "Command 'quickstart' failed and the partially created instance could \
                     not be trashed: {detail}"
                );
            }
        }
    }

    outcome
}

#[allow(clippy::too_many_arguments)]
async fn run_quickstart(
    instance: Option<String>,
    cluster: ClusterArguments,
    port: u16,
    save: Option<bool>,
    details: Option<bool>,
    yes: Option<bool>,
    loglevel: Option<LogLevel>,
    overrides: &HashMap<String, String>,
    globals: &GlobalArguments,
) -> CommandResult {
    command_init(
        InitArguments {
            instance: instance.clone(),
            cluster,
            details,
            save,
            yes,
            loglevel,
        },
        globals,
    )
    .await?;

    let pins = PinContext::pins();
    let resolved_instance_value = resolved_instance(instance.clone(), &pins);
    let resolved_details_value = resolved_details(details, &pins);
    let resolved_yes_value = resolved_yes(yes, &pins);
    let resolved_verbose_value = globals.verbose;

    create_instance(
        resolved_instance_value.value,
        resolved_instance_value.pinned,
        port,
        resolved_details_value.value,
        resolved_yes_value.value,
        resolved_verbose_value,
        overrides,
    )
    .await?;

    command_start(
        StartArguments {
            instance,
            loglevel: None,
            details,
            yes,
        },
        globals,
    )
    .await
}
