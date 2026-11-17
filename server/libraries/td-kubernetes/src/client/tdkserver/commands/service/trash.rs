//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Local;
use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_error::TdError;
use tracing::info;

use crate::base::error::cli::CliError;
use crate::base::error::manifest::ManifestError;
use crate::base::reporter::InteractiveReporter;
use crate::client::registry::{InstanceSettings, instance_folder, trash_root};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::service::{
    resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::preview;
use crate::core::system::exit::{CommandError, CommandResult, render_error};
use crate::cwarn;
use crate::runtime::instance::verify_instance_version;
use crate::runtime::provider::kminus::KminusProvider;
use crate::runtime::provider::{
    ClusterProvider, Provider, provider_for_instance_mutating_any_state,
};

const TRASH_TIMESTAMP_FORMAT: &str = "%Y.%m.%d.%H.%M.%S%.3f";
const COMPACT_TRASH_TIMESTAMP_FORMAT: &str = "%Y%m%d%H%M%S%3f";
const COMPACT_ENV: &str = "TD_KMINUS_COMPACT";

const TRASH_ANYWAY_PROMPT: &str = "Trash the instance anyway?";
const STOP_FAILED_WARNING: &str = "A failed stop can leave processes running and stale files \
     behind (pid files, an orphaned database server, bound ports). On Windows, files still held \
     open by those processes can make the trash fail.";

const TRASH_RENAME_BACKOFF: Duration = Duration::from_millis(250);
#[cfg(not(windows))]
const TRASH_RENAME_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(windows)]
const TRASH_RENAME_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Args)]
pub struct TrashArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to trash. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before trashing the instance. \
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Lineage {
    Matching,
    Mismatched {
        found: String,
        running: String,
        provider: ClusterProvider,
    },
    Unreadable {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopPlan {
    Full,
    BestEffort,
    Unsupported(ClusterProvider),
}

impl Lineage {
    fn of(name: &str, verbose: bool) -> Self {
        let settings = match InstanceSettings::load(name) {
            Ok(settings) => settings,
            Err(error) => {
                return Self::Unreadable {
                    reason: render(&error, verbose),
                };
            }
        };
        match verify_instance_version(&settings.name, &settings.version) {
            Ok(()) => Self::Matching,
            Err(ManifestError::InstanceVersionMismatch { found, running, .. }) => {
                Self::Mismatched {
                    found,
                    running,
                    provider: settings.provider,
                }
            }
            Err(error) => Self::Unreadable {
                reason: error.to_string(),
            },
        }
    }

    fn stop_plan(&self) -> StopPlan {
        match self {
            Self::Matching => StopPlan::Full,
            Self::Mismatched {
                provider: ClusterProvider::Kminus,
                ..
            }
            | Self::Unreadable { .. } => StopPlan::BestEffort,
            Self::Mismatched { provider, .. } => StopPlan::Unsupported(*provider),
        }
    }

    fn warning(&self, name: &str) -> Option<String> {
        match self {
            Self::Matching => None,
            Self::Mismatched { found, running, .. } => Some(format!(
                "Instance '{name}' was created with tabsdata {found}, but this tdkserver is \
                 {running}. It only gets a best-effort stop before it is trashed."
            )),
            Self::Unreadable { reason } => Some(format!(
                "The tabsdata version of instance '{name}' is unknown, as its settings could \
                 not be read ({reason}). It only gets a best-effort stop before it is trashed."
            )),
        }
    }
}

pub(crate) async fn prepare_trash(
    name: &str,
    yes: bool,
    verbose: bool,
    prompt: Option<&str>,
) -> CommandResult<bool> {
    let lineage = Lineage::of(name, verbose);
    let approved = match (lineage.warning(name), prompt) {
        (Some(warning), _) => {
            cwarn!("{warning}");
            confirm(yes, TRASH_ANYWAY_PROMPT)?
        }
        (None, Some(prompt)) => confirm(yes, prompt)?,
        (None, None) => true,
    };
    if !approved {
        return Ok(false);
    }
    let Err(reason) = stop_for_trash(name, &lineage, verbose).await else {
        return Ok(true);
    };
    cwarn!("Could not stop instance '{name}': {reason}");
    cwarn!("{STOP_FAILED_WARNING}");
    confirm(yes, TRASH_ANYWAY_PROMPT)
}

async fn stop_for_trash(name: &str, lineage: &Lineage, verbose: bool) -> Result<(), String> {
    let stopped = match lineage.stop_plan() {
        StopPlan::Full => stop_fully(name, verbose).await,
        StopPlan::BestEffort => stop_kminus_cluster(name, verbose).await,
        StopPlan::Unsupported(provider) => {
            return Err(format!(
                "this tdkserver cannot stop a {} instance created by another tabsdata \
                 version; its workload is left untouched",
                provider.as_str()
            ));
        }
    };
    stopped.map_err(|error| render(&error, verbose))
}

async fn stop_fully(name: &str, verbose: bool) -> CommandResult {
    let provider = provider_for_instance_mutating_any_state(Some(name.to_owned()), verbose).await?;
    provider.ensure_stoppable_service().await?;
    if provider.service_dead().await {
        return Ok(());
    }
    provider.stop_service(&InteractiveReporter::new()).await?;
    Ok(())
}

async fn stop_kminus_cluster(name: &str, verbose: bool) -> CommandResult {
    let provider = KminusProvider::build(
        ClusterArguments::new(Some(ClusterProvider::Kminus), None, None, false, None),
        InstanceArguments::new(Some(name.to_owned())),
        verbose,
        None,
        None,
    )?;
    provider.stop_cluster(&InteractiveReporter::new()).await?;
    Ok(())
}

pub async fn command_trash(arguments: TrashArguments, globals: &GlobalArguments) -> CommandResult {
    let TrashArguments {
        instance,
        details,
        yes,
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let verbose = globals.verbose;

    let name = instance
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());

    let folder = instance_folder(&name)?;
    if !folder.exists() {
        cwarn!("Instance '{name}' does not exist. Nothing to trash.");
        return Ok(());
    }

    let trash_root = trash_root()?;

    if details.value {
        preview::trash_service(
            &name,
            &folder.display().to_string(),
            &trash_root.display().to_string(),
        );
    }
    if !prepare_trash(
        &name,
        yes.value,
        verbose,
        Some("Proceed with trashing the instance?"),
    )
    .await?
    {
        info!("Operation cancelled.");
        return Ok(());
    }

    let destination = trash_instance_folder(&name, &folder)?;
    info!(
        "Instance '{name}' moved to trash: {}",
        destination.display()
    );

    Ok(())
}

// Without separators on Windows, whose paths are capped at 260 characters,
// and wherever TD_KMINUS_COMPACT is true
fn trash_timestamp_format(windows: bool, compact: Option<&str>) -> &'static str {
    let compact = compact.is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
    if windows || compact {
        COMPACT_TRASH_TIMESTAMP_FORMAT
    } else {
        TRASH_TIMESTAMP_FORMAT
    }
}

pub fn trash_instance_folder(name: &str, folder: &Path) -> CommandResult<PathBuf> {
    let trash_root = trash_root()?;
    fs::create_dir_all(&trash_root).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: trash_root.display().to_string(),
            cause: error.to_string(),
        })
    })?;

    let format = trash_timestamp_format(cfg!(windows), std::env::var(COMPACT_ENV).ok().as_deref());
    let timestamp = Local::now().format(format).to_string();
    let destination = trash_root.join(format!("{name}-{timestamp}"));

    let deadline = Instant::now() + TRASH_RENAME_TIMEOUT;
    let mut last_error;
    loop {
        match fs::rename(folder, &destination) {
            Ok(()) => return Ok(destination),
            Err(error) => last_error = error,
        }
        if Instant::now() + TRASH_RENAME_BACKOFF >= deadline {
            break;
        }
        std::thread::sleep(TRASH_RENAME_BACKOFF);
    }

    Err(TdError::from(CliError::FileWriteFailed {
        file: destination.display().to_string(),
        cause: last_error.to_string(),
    })
    .into())
}

fn render(error: &CommandError, verbose: bool) -> String {
    match error.source() {
        Some(source) => render_error(source, verbose),
        None => error.status().to_string(),
    }
}

#[cfg(test)]
mod tests;
