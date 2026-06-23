//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;

use crate::base::constants::{SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments};
use crate::client::tdkserver::commands::service::create::create_instance;
use crate::client::tdkserver::commands::service::init::{InitArguments, command_init};
use crate::client::tdkserver::commands::service::start::{StartArguments, command_start};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::core::system::exit::{CommandResult, abort};
use crate::runtime::instance::ports::DEFAULT_BASE_PORT;

#[derive(Debug, Clone, Args)]
pub struct QuickstartArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Name of the instance to initialize, create and start. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[arg(
        long,
        name = "port",
        required = false,
        default_value_t = DEFAULT_BASE_PORT,
        long_help = "Base HTTP/HTTPS port. Ports for the instance services are assigned \
                     sequentially starting from this value."
    )]
    pub port: u16,

    #[arg(
        long,
        name = "openai-key",
        required = false,
        long_help = "OpenAI API key for the aiagent. Optional."
    )]
    pub openai_key: Option<String>,

    #[arg(
        long,
        name = "anthropic-key",
        required = false,
        long_help = "Anthropic API key for the aiagent. Optional."
    )]
    pub anthropic_key: Option<String>,

    #[arg(
        long,
        name = "save",
        required = false,
        default_value_t = false,
        long_help = "Copy the cluster settings files (kubeconfig, etc.) into the instance \
                     folder so the instance is self-contained."
    )]
    pub save: bool,
}

pub async fn command_quickstart(
    arguments: QuickstartArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let QuickstartArguments {
        instance,
        cluster,
        port,
        openai_key,
        anthropic_key,
        save,
    } = arguments;

    let mut overrides: HashMap<String, String> = HashMap::new();
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

    command_init(
        InitArguments {
            instance: instance.clone(),
            cluster,
            preview: false,
            save,
            yes: true,
        },
        globals,
    )
    .await?;

    create_instance(instance.clone(), port, &overrides, globals).await?;

    command_start(
        StartArguments {
            instance,
            preview: false,
            yes: true,
        },
        globals,
    )
    .await
}
