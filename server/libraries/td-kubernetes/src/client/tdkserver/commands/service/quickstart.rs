//
// Copyright 2026 Tabsdata Inc.
//

/*
use std::path::Path;
*/
use std::path::{Path, PathBuf};

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::{Cancelled, NoAction};
use td_error::TdError;
use tracing::{info, warn};

/*
#[cfg(feature = "enterprise")]
use crate::base::constants::REQUIREMENTS_AG_ASSET;
use crate::base::constants::{
    KPROFILE_ASSETS_FAMILY, REQUIREMENTS_FN_ASSET, REQUIREMENTS_TD_ASSET,
};
*/
use crate::base::constants::{
    ENV_TD_KUBE_UV_MODE, ENV_UV_INDEX, FILL_STUB_FILE, LogLevel, STUB_FILE_PREFIX, STUBS_FOLDER,
    UvMode,
};
#[cfg(feature = "enterprise")]
use crate::base::constants::{SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY};
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::client::registry::{fill_root, instance_folder, instance_init_fill_folder};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments};
use crate::client::tdkserver::commands::service::create::create_instance;
use crate::client::tdkserver::commands::service::delete::delete_if_possible;
use crate::client::tdkserver::commands::service::init::{InitArguments, init_instance};
use crate::client::tdkserver::commands::service::start::{StartArguments, command_start};
use crate::client::tdkserver::commands::service::trash::{stop_if_possible, trash_instance_folder};
use crate::client::tdkserver::commands::service::{
    resolved_cluster, resolved_details, resolved_instance, resolved_yes,
};
#[cfg(feature = "enterprise")]
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::common::interactive;
use crate::client::tdkserver::filler::{Heading, Outcome, fill};
use crate::client::tdkserver::injector::{
    self, Address, CONFIG_ARGUMENT, CONFIG_EXTENSION, CONFIG_HELP, CONFIG_LONG_HELP, FILE_PREFIX,
    Provided, Setting, key_address, parse_settings,
};
#[allow(unused_imports)]
use crate::client::tdkserver::widgets::show_notice;
use crate::core::system::exit::{CommandResult, abort, abort_with, render_error};
use crate::cwarn;
#[cfg(feature = "enterprise")]
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::instance::index;
use crate::runtime::instance::ports::DEFAULT_BASE_PORT;
use crate::runtime::instance::profile::{load_profile, resolve_kprofile_path};
use crate::runtime::instance::tabsdata::TABSDATA_FILE;
use crate::runtime::provider::ClusterProvider;

const FILL_ARGUMENT: &str = "fill";

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
        display_order = 120,
        long_help = "Deprecated: use '--config anthropic-key=<KEY>' instead. Anthropic API key \
                     for the aiagent. Optional."
    )]
    pub anthropic_key: Option<String>,

    #[cfg(feature = "enterprise")]
    #[arg(
        long,
        name = "openai-key",
        required = false,
        display_order = 130,
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
        display_order = 170,
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
        display_order = 180,
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
        display_order = 190,
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
        display_order = 110,
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
        long = FILL_ARGUMENT,
        name = FILL_ARGUMENT,
        required = false,
        display_order = 100,
        help = "Fill the configuration stub of the provider in a built-in editor before \
                anything is created. Run with '--help' for details.",
        long_help = "Fill the configuration stub of the provider in a built-in editor before \
                     anything is created. The editor opens the stub of the chosen provider, \
                     or the file left by an earlier '--fill' of the same instance, with the \
                     required values marked. Ctrl+Q saves and continues once the settings \
                     are valid, Ctrl+S saves without leaving, and Esc quits without saving, \
                     which cancels the quickstart.\n\n\
                     Editing keys use Ctrl, also on macOS: the terminal keeps Cmd for \
                     itself. Ctrl+C, Ctrl+X and Ctrl+V use the system clipboard, so text \
                     moves between the editor and other applications, and Cmd+V pastes \
                     too. Ctrl+A selects all; Shift with the arrows, or a mouse drag, \
                     selects a block.\n\n\
                     The filled file is used as '--config @FILE', together with any other \
                     '--config' settings, and it is kept in the 'init/fill' folder of the \
                     instance.\n\n\
                     Example:\n  \
                     tdkserver quickstart --provider gke --fill"
    )]
    pub fill: bool,

    #[arg(
        long,
        name = "save",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 140,
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
        display_order = 150,
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
        display_order = 160,
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
        fill,
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

    let name = resolved_instance(instance.clone(), &settings)
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let folder = instance_folder(&name)?;
    let preexisting = folder.exists();

    let mut cluster = cluster;
    let provider = cluster
        .provider
        .or_else(|| resolved_cluster(cluster.clone(), &settings).0.provider)
        .unwrap_or(ClusterProvider::Kminus);
    if !provider.supported() {
        return abort_with(
            NoAction,
            ProviderError::CommandNotSupportedForProvider {
                command: "quickstart".to_owned(),
                provider: provider.as_str().to_owned(),
            },
        );
    }
    cluster.provider = Some(provider);

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
    }
    plan_uv_index(provider, uv_index, &mut injections)?;
    let filled = if fill {
        let Some(staged) = fill_settings(provider, &name, &mut injections, globals.verbose)? else {
            return abort(Cancelled);
        };
        Some(staged)
    } else {
        None
    };
    #[cfg(feature = "enterprise")]
    {
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

    let outcome = run_quickstart(
        instance,
        cluster,
        port,
        save,
        details,
        yes,
        loglevel,
        /*
        requirements_td,
        requirements_fn,
        #[cfg(feature = "enterprise")]
        requirements_ag,
        */
        injections,
        filled.as_deref(),
        globals,
    )
    .await;

    if outcome.is_ok()
        && let Some(staged) = &filled
        && let Err(error) = std::fs::remove_file(staged)
    {
        cwarn!(
            "Could not remove the filled settings file '{}': {error}",
            staged.display()
        );
    }

    if outcome.is_err() && !preexisting && folder.exists() {
        if provider == ClusterProvider::Kminus {
            stop_if_possible(&name, globals.verbose).await;
        } else {
            delete_if_possible(&name, globals.verbose).await;
        }
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

fn fill_settings(
    provider: ClusterProvider,
    name: &str,
    injections: &mut Vec<Setting>,
    verbose: bool,
) -> CommandResult<Option<PathBuf>> {
    if !interactive() {
        return Err(TdError::from(CliError::FillNeedsTerminal).into());
    }
    let staged = fill_root()?.join(format!("{name}{CONFIG_EXTENSION}"));
    if !staged.exists() {
        stage_stub(provider, &staged, verbose)?;
    }
    let profile = load_profile(provider, verbose).map_err(TdError::from)?;
    let argument = format!("{FILE_PREFIX}{}", staged.display());
    let heading = Heading {
        instance: name.to_owned(),
        provider: provider.as_str().to_owned(),
    };
    info!(
        "Opening the settings of instance '{name}' in the editor: {}",
        staged.display()
    );
    let validate = |_: &Path| -> Result<(), String> {
        let mut settings = injections.clone();
        settings
            .extend(parse_settings(std::slice::from_ref(&argument)).map_err(|e| e.to_string())?);
        injector::plan(&profile.configs, &settings)
            .map(|_| ())
            .map_err(|error| error.to_string())
    };
    if fill(&staged, &heading, validate).map_err(TdError::from)? == Outcome::Discarded {
        return Ok(None);
    }
    injections.extend(parse_settings(&[argument]).map_err(TdError::from)?);
    info!("Using the filled settings: {}", staged.display());
    Ok(Some(staged))
}

fn stage_stub(provider: ClusterProvider, staged: &Path, verbose: bool) -> CommandResult {
    let stub = resolve_kprofile_path(verbose)
        .map_err(TdError::from)?
        .join(STUBS_FOLDER)
        .join(format!(
            "{STUB_FILE_PREFIX}{}{CONFIG_EXTENSION}",
            provider.as_str()
        ));
    if let Some(parent) = staged.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            TdError::from(CliError::FolderCreateFailed {
                folder: parent.display().to_string(),
                cause: error.to_string(),
            })
        })?;
    }
    std::fs::copy(&stub, staged).map_err(|error| {
        TdError::from(CliError::FileReadFailed {
            file: stub.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    Ok(())
}

fn keep_filled(name: &str, staged: &Path) -> CommandResult {
    let folder = instance_init_fill_folder(name)?;
    std::fs::create_dir_all(&folder).map_err(|error| {
        TdError::from(CliError::FolderCreateFailed {
            folder: folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    let target = folder.join(FILL_STUB_FILE);
    std::fs::copy(staged, &target).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: target.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    Ok(())
}

fn deprecated(argument: &str, address: Address, value: String) -> Setting {
    cwarn!(
        "Argument '--{argument}' is deprecated; use '--{CONFIG_ARGUMENT} {address}=<value>' instead."
    );
    Setting::new(address, Provided::Text(value), format!("'--{argument}'"))
}

fn plan_uv_index(
    provider: ClusterProvider,
    uv_index: Option<String>,
    injections: &mut Vec<Setting>,
) -> CommandResult {
    let uv_mode = UvMode::from_environment().map_err(TdError::from)?;
    let governed = index::governed(provider, uv_mode);
    let uv_index = uv_index.filter(|index| !index.trim().is_empty());
    if let Some(index) = uv_index {
        if !governed {
            return Err(TdError::from(CliError::UvIndexNotGoverned {
                variable: ENV_TD_KUBE_UV_MODE.to_owned(),
                provider: provider.as_str().to_owned(),
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
    filled: Option<&Path>,
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

    if let Some(staged) = filled {
        let name = resolved_instance(instance.clone(), &SettingsContext::settings())
            .value
            .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
        keep_filled(&name, staged)?;
    }

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
