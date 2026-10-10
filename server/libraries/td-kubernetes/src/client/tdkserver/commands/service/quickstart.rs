//
// Copyright 2026 Tabsdata Inc.
//

/*
use std::path::Path;
*/
use std::path::PathBuf;

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
#[cfg(feature = "enterprise")]
use td_common::status::ExitStatus::Cancelled;
use td_common::status::ExitStatus::NoAction;
use td_error::TdError;
use tracing::{info, warn};

/*
#[cfg(feature = "enterprise")]
use crate::base::constants::REQUIREMENTS_AG_ASSET;
use crate::base::constants::{
    KPROFILE_ASSETS_FAMILY, REQUIREMENTS_FN_ASSET, REQUIREMENTS_TD_ASSET,
};
*/
use crate::base::constants::{ENV_TD_KUBE_UV_MODE, ENV_UV_INDEX, LogLevel, UvMode};
#[cfg(feature = "enterprise")]
use crate::base::constants::{SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY};
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::client::registry::instance_folder;
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments};
use crate::client::tdkserver::commands::service::create::create_instance;
use crate::client::tdkserver::commands::service::init::{InitArguments, init_instance};
use crate::client::tdkserver::commands::service::start::{StartArguments, command_start};
use crate::client::tdkserver::commands::service::trash::{stop_if_possible, trash_instance_folder};
use crate::client::tdkserver::commands::service::{
    resolved_details, resolved_instance, resolved_yes,
};
#[cfg(feature = "enterprise")]
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::injector::{
    Address, CONFIG_ARGUMENT, CONFIG_HELP, CONFIG_LONG_HELP, Provided, Setting, key_address,
    parse_settings,
};
#[allow(unused_imports)]
use crate::client::tdkserver::widgets::show_notice;
#[cfg(feature = "enterprise")]
use crate::core::system::exit::abort;
use crate::core::system::exit::{CommandResult, abort_with, render_error};
use crate::cwarn;
#[cfg(feature = "enterprise")]
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::instance::index;
use crate::runtime::instance::ports::DEFAULT_BASE_PORT;
use crate::runtime::instance::tabsdata::TABSDATA_FILE;
use crate::runtime::provider::ClusterProvider;

const QUICKSTART_PROVIDER: ClusterProvider = ClusterProvider::Kminus;

const UV_INDEX_KEYS: [&str; 4] = ["pods", "global", "envs", ENV_UV_INDEX];

#[allow(dead_code)]
const VAULT_DISCLAIMER: &str = "The database passwords this quickstart generated are stored in \
     the instance's '.vault.json'. Keep that file: creating this instance again reuses those \
     values, and without them the database roles left behind are unreachable. Plain-text local \
     storage is meant for trying tabsdata out, never for a production or otherwise sensitive \
     instance.";

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
        display_order = 80,
        help = "Log level for the kminus orchestrator and pod runner. Defaults to \
                'debug'. Only valid with the 'kminus' provider.",
        long_help = "Log level for the kminus orchestrator and pod runner. \
                     Defaults to 'debug'. Only valid with the 'kminus' provider.\n\n\
                     Example:\n  \
                     tdkserver quickstart --provider kminus --loglevel info"
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
        name = "anthropic-key",
        required = false,
        display_order = 110,
        long_help = "Deprecated: use '--config anthropic-key=<KEY>' instead. Anthropic API key \
                     for the aiagent. Optional."
    )]
    pub anthropic_key: Option<String>,

    #[cfg(feature = "enterprise")]
    #[arg(
        long,
        name = "openai-key",
        required = false,
        display_order = 120,
        long_help = "Deprecated: use '--config openai-key=<KEY>' instead. OpenAI API key for \
                     the aiagent. Optional."
    )]
    pub openai_key: Option<String>,

    /*
    #[arg(
        long = "reqs-td",
        name = "reqs-td",
        required = false,
        value_name = "FILE",
        display_order = 160,
        long_help = "Complementary packages for the td virtual environment. The file \
                     replaces the empty requirements-td.txt of the initialized instance \
                     and is installed when the virtual environment is built."
    )]
    */
    #[arg(long = "reqs-td", name = "reqs-td", required = false, hide = true)]
    pub requirements_td: Option<PathBuf>,

    /*
    #[arg(
        long = "reqs-fn",
        name = "reqs-fn",
        required = false,
        value_name = "FILE",
        display_order = 170,
        long_help = "Complementary packages for the fn virtual environment. The file \
                     replaces the empty requirements-fn.txt of the initialized instance \
                     and is installed when the virtual environment is built."
    )]
    */
    #[arg(long = "reqs-fn", name = "reqs-fn", required = false, hide = true)]
    pub requirements_fn: Option<PathBuf>,

    /*
    #[cfg(feature = "enterprise")]
    #[arg(
        long = "reqs-ag",
        name = "reqs-ag",
        required = false,
        value_name = "FILE",
        display_order = 180,
        long_help = "Complementary packages for the ag virtual environment. The file \
                     replaces the empty requirements-ag.txt of the initialized instance \
                     and is installed when the virtual environment is built."
    )]
    */
    #[cfg(feature = "enterprise")]
    #[arg(long = "reqs-ag", name = "reqs-ag", required = false, hide = true)]
    pub requirements_ag: Option<PathBuf>,

    #[arg(
        long = "uv-index",
        name = "uv-index",
        required = false,
        value_name = "URI",
        display_order = 100,
        help = "Deprecated: use '--config tabsdata:pods.global.envs.UV_INDEX=<URI>' \
                instead. Python package index for the instance. Run with '--help' for \
                details.",
        long_help = "Deprecated: use '--config tabsdata:pods.global.envs.UV_INDEX=<URI>' \
                     instead. Python package index for the instance. The URI is set as the \
                     'UV_INDEX' \
                     global environment variable of the initialized instance, which every \
                     container of every pod receives, and it takes precedence over the \
                     'UV_INDEX' and 'PIP_INDEX_URL' variables of this machine.\n\n\
                     Only valid with 'TD_KUBE_UV_MODE' unset, because only then are the \
                     packages resolved from this machine's environment.\n\n\
                     Example:\n  \
                     tdkserver quickstart --uv-index http://localhost:8000/+simple"
    )]
    pub uv_index: Option<String>,

    #[arg(
        long = CONFIG_ARGUMENT,
        name = CONFIG_ARGUMENT,
        value_name = "SETTING",
        required = false,
        action = clap::ArgAction::Append,
        display_order = 90,
        help = CONFIG_HELP,
        long_help = CONFIG_LONG_HELP
    )]
    pub config: Vec<String>,

    #[arg(
        long,
        name = "save",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 130,
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
        display_order = 140,
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
        display_order = 150,
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
        anthropic_key,
        #[cfg(feature = "enterprise")]
        openai_key,
        save,
        details,
        yes,
        loglevel,
        requirements_td,
        requirements_fn,
        #[cfg(feature = "enterprise")]
        requirements_ag,
        uv_index,
        config,
    } = arguments;

    #[cfg_attr(not(feature = "enterprise"), allow(unused_mut))]
    let mut unsupported: Vec<&str> = Vec::new();
    if requirements_td.is_some() {
        unsupported.push("reqs-td");
    }
    if requirements_fn.is_some() {
        unsupported.push("reqs-fn");
    }
    #[cfg(feature = "enterprise")]
    if requirements_ag.is_some() {
        unsupported.push("reqs-ag");
    }
    if let Some(argument) = unsupported.first() {
        return abort_with(
            NoAction,
            CliError::ArgumentNoLongerSupported {
                argument: (*argument).to_owned(),
            },
        );
    }

    let settings = SettingsContext::settings();

    let mut injections = parse_settings(&config).map_err(TdError::from)?;
    #[cfg(feature = "enterprise")]
    {
        let keys = [
            (
                "anthropic-key",
                SECRET_CATEGORY_ANTHROPIC_KEY,
                anthropic_key,
            ),
            ("openai-key", SECRET_CATEGORY_OPENAI_KEY, openai_key),
        ];
        for (argument, category, value) in keys {
            if let Some(value) = value {
                let address = Address::Category {
                    kind: Some(PlaceholderKind::Secret),
                    category: category.to_owned(),
                };
                injections.push(deprecated(argument, address, value));
            }
        }
        let keyed = injections.iter().any(|injection| {
            [SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY]
                .iter()
                .any(|category| injection.names_category(PlaceholderKind::Secret, category))
        });
        if !keyed
            && !confirm(
                resolved_yes(yes, &settings).value,
                "No Anthropic or OpenAI key provided. Proceed with no aiagent keys?",
            )?
        {
            return abort(Cancelled);
        }
    }
    plan_uv_index(uv_index, &mut injections)?;

    let name = resolved_instance(instance.clone(), &settings)
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let folder = instance_folder(&name)?;
    let preexisting = folder.exists();

    let mut cluster = cluster;
    match cluster.provider {
        None => cluster.provider = Some(QUICKSTART_PROVIDER),
        Some(provider) if provider != QUICKSTART_PROVIDER => {
            return abort_with(
                NoAction,
                ProviderError::CommandNotSupportedForProvider {
                    command: "quickstart".to_owned(),
                    provider: provider.as_str().to_owned(),
                },
            );
        }
        Some(_) => {}
    }
    let outcome = run_quickstart(
        instance, cluster, port, save, details, yes, loglevel,
        /*
        requirements_td,
        requirements_fn,
        #[cfg(feature = "enterprise")]
        requirements_ag,
        */
        injections, globals,
    )
    .await;

    if outcome.is_err() && !preexisting && folder.exists() {
        stop_if_possible(&name, globals.verbose).await;
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

/*
fn persist_requirements(instance: &str, asset: &str, source: &Path) -> CommandResult {
    let contents = std::fs::read_to_string(source).map_err(|error| {
        TdError::from(CliError::FileReadFailed {
            file: source.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    let target = instance_init_input_folder(instance)?
        .join(KPROFILE_ASSETS_FAMILY.folder)
        .join(asset);
    std::fs::write(&target, contents).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: target.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    info!("Wrote {} from {}", asset, source.display());
    Ok(())
}
*/

fn deprecated(argument: &str, address: Address, value: String) -> Setting {
    cwarn!(
        "Argument '--{argument}' is deprecated; use '--{CONFIG_ARGUMENT} {address}=<value>' instead."
    );
    Setting::new(address, Provided::Text(value), format!("'--{argument}'"))
}

fn plan_uv_index(uv_index: Option<String>, injections: &mut Vec<Setting>) -> CommandResult {
    let uv_mode = UvMode::from_environment().map_err(TdError::from)?;
    let governed = index::governed(QUICKSTART_PROVIDER, uv_mode);
    let uv_index = uv_index.filter(|index| !index.trim().is_empty());
    if let Some(index) = uv_index {
        if !governed {
            return Err(TdError::from(CliError::UvIndexNotGoverned {
                variable: ENV_TD_KUBE_UV_MODE.to_owned(),
                provider: QUICKSTART_PROVIDER.as_str().to_owned(),
                mode: uv_mode.as_str().to_owned(),
            })
            .into());
        }
        info!("The instance will use the Python package index: {index} (from --uv-index)");
        injections.push(deprecated(
            "uv-index",
            key_address(TABSDATA_FILE, &UV_INDEX_KEYS),
            index,
        ));
        return Ok(());
    }
    let explicit = injections
        .iter()
        .any(|injection| injection.names_key(TABSDATA_FILE, &UV_INDEX_KEYS));
    if !governed || explicit {
        return Ok(());
    }
    let plan = index::resolve(None, None);
    if let Some(local) = plan.local.clone() {
        info!(
            "The instance will use the Python package index: {}",
            plan.obtain_local()
        );
        injections.push(Setting::new(
            key_address(TABSDATA_FILE, &UV_INDEX_KEYS),
            Provided::Text(local),
            format!("'{}'", plan.origin.as_str()),
        ));
    }
    Ok(())
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
    /*
    requirements_td: Option<PathBuf>,
    requirements_fn: Option<PathBuf>,
    #[cfg(feature = "enterprise")]
    requirements_ag: Option<PathBuf>,
    */
    injections: Vec<Setting>,
    globals: &GlobalArguments,
) -> CommandResult {
    init_instance(
        InitArguments {
            instance: instance.clone(),
            cluster,
            details,
            save,
            yes,
            loglevel,
            config: Vec::new(),
        },
        globals,
        false,
        injections,
    )
    .await?;

    /*
    let target_instance = resolved_instance(instance.clone(), &SettingsContext::settings())
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    if let Some(source) = &requirements_td {
        persist_requirements(&target_instance, REQUIREMENTS_TD_ASSET, source)?;
    }
    if let Some(source) = &requirements_fn {
        persist_requirements(&target_instance, REQUIREMENTS_FN_ASSET, source)?;
    }
    #[cfg(feature = "enterprise")]
    if let Some(source) = &requirements_ag {
        persist_requirements(&target_instance, REQUIREMENTS_AG_ASSET, source)?;
    }
    */

    let settings = SettingsContext::settings();
    let resolved_instance_value = resolved_instance(instance.clone(), &settings);
    let resolved_details_value = resolved_details(details, &settings);
    let resolved_yes_value = resolved_yes(yes, &settings);
    let resolved_verbose_value = globals.verbose;

    create_instance(
        resolved_instance_value.value,
        resolved_instance_value.in_settings,
        port,
        resolved_details_value.value,
        resolved_yes_value.value,
        resolved_verbose_value,
        false,
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
