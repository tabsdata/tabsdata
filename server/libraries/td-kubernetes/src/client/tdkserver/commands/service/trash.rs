//
// Copyright 2026 Tabs Data Inc.
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
use crate::base::reporter::InteractiveReporter;
use crate::client::registry::{InstanceSettings, instance_folder, trash_root};
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::preview;
use crate::core::system::exit::{CommandError, CommandResult, render_error};
use crate::cwarn;
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::provider_for_instance;

const TRASH_TIMESTAMP_FORMAT: &str = "%Y.%m.%d.%H.%M.%S%.3f";

const TRASH_RENAME_BACKOFF: Duration = Duration::from_millis(250);
#[cfg(not(windows))]
const TRASH_RENAME_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(windows)]
const TRASH_RENAME_TIMEOUT: Duration = Duration::from_secs(300);

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

pub async fn command_trash(arguments: TrashArguments, globals: &GlobalArguments) -> CommandResult {
    let TrashArguments {
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
    if !confirm(yes.value, "Proceed with trashing the instance?")? {
        info!("Operation cancelled.");
        return Ok(());
    }

    stop_if_possible(&name, verbose).await;

    let destination = trash_instance_folder(&name, &folder)?;
    info!(
        "Instance '{name}' moved to trash: {}",
        destination.display()
    );

    Ok(())
}

pub fn trash_instance_folder(name: &str, folder: &Path) -> CommandResult<PathBuf> {
    let trash_root = trash_root()?;
    fs::create_dir_all(&trash_root).map_err(|error| {
        TdError::from(CliError::FileWriteFailed {
            file: trash_root.display().to_string(),
            cause: error.to_string(),
        })
    })?;

    let timestamp = Local::now().format(TRASH_TIMESTAMP_FORMAT).to_string();
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

async fn stop_if_possible(name: &str, verbose: bool) {
    let settings = match InstanceSettings::load(name) {
        Ok(settings) => settings,
        Err(error) => {
            cwarn!(
                "Skipping stop; could not read instance settings: {}",
                render(&error, verbose)
            );
            return;
        }
    };

    if settings.provider != ClusterProvider::Kminus {
        info!("Skipping stop; instance provider is not a local kminus instance.");
        return;
    }

    let provider = match provider_for_instance(Some(name.to_owned()), verbose, None).await {
        Ok(provider) => provider,
        Err(error) => {
            cwarn!(
                "Skipping stop; could not prepare the instance to stop it: {}",
                render(&error, verbose)
            );
            return;
        }
    };

    if provider.service_dead().await {
        cwarn!("Instance '{name}' is already stopped; skipping stop.");
        return;
    }

    let reporter = InteractiveReporter::new();
    if let Err(error) = provider.stop_service(&reporter).await {
        cwarn!(
            "Could not stop instance before trashing it: {}",
            render(&error, verbose)
        );
    }
}

fn render(error: &CommandError, verbose: bool) -> String {
    match error.source() {
        Some(source) => render_error(source, verbose),
        None => error.status().to_string(),
    }
}
