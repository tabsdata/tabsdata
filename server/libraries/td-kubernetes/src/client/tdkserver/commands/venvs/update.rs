//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap_derive::Args;
use pep508_rs::{Requirement, VerbatimUrl};
use td_common::status::ExitStatus::Cancelled;
use td_error::TdError;
use tracing::info;

use crate::base::constants::{
    ENV_TD_KUBE_UV_MODE, KPROFILE_ASSETS_FAMILY, UV_MODE_LINKED, UvMode, Venv,
};
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::{confirm, confirm_at_leisure};
use crate::client::tdkserver::render::report;
use crate::core::system::exit::{CommandResult, abort};
use crate::kube::api::configmap::replace_configmap;
use crate::kube::context::KubeContext;
use crate::runtime::provider::{ClusterProvider, Provider, provider_for_instance_mutating};

const REQUIREMENTS_OPTION_PREFIX: char = '-';

#[derive(Debug, Clone, Args)]
pub struct UpdateArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose virtual environment is updated. \
                     Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "name",
        required = true,
        value_enum,
        display_order = 20,
        long_help = "Virtual environment to update. Run 'tdkserver venv list' to see the \
                     virtual environments and what each one is used for."
    )]
    pub name: Venv,

    #[arg(
        long,
        name = "requirements",
        required = true,
        value_name = "FILE",
        display_order = 30,
        long_help = "File with the complementary packages of the virtual environment, in pip \
                     requirements format. It replaces the requirements file the instance \
                     currently holds for the virtual environment."
    )]
    pub requirements: PathBuf,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before the update. Defaults to \
                     'true'. Pass '--details false' to skip the summary."
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

pub async fn command_venv_update(
    arguments: UpdateArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let UpdateArguments {
        instance,
        name,
        requirements,
        details,
        yes,
    } = arguments;

    let contents = read_requirements(&requirements)?;
    validate_requirements(&requirements, &contents)?;

    if UvMode::from_environment().map_err(TdError::from)? == UvMode::Linked {
        return Err(ProviderError::VenvUpdateUnsupportedWhenLinked {
            variable: ENV_TD_KUBE_UV_MODE.to_owned(),
            linked: UV_MODE_LINKED.to_owned(),
            requirements: requirements.display().to_string(),
        }
        .into());
    }

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let verbose = globals.verbose;

    let provider = provider_for_instance_mutating(instance.value, verbose, None).await?;
    let instance_name = provider.view().instance_name.unwrap_or_default();
    let asset = name.requirements_asset();
    let quiesces = provider.cluster_provider() == ClusterProvider::Kminus;

    let standing = if quiesces {
        Standing::observe(provider.as_ref()).await
    } else {
        Standing::running()
    };

    if details.value {
        info!(
            "About to update the '{}' virtual environment of tabsdata instance \
             '{instance_name}':",
            name.as_str()
        );
        info!("  - {} from {}", asset, requirements.display());
        info!("The tabsdata python environments will be rebuilt afterwards.");
        if quiesces && standing.instance {
            info!(
                "The instance is running. It will be stopped while the virtual \
                 environments are rebuilt, and started again afterwards."
            );
        }
    }
    if !confirm(yes.value, "Proceed with the update?")? {
        info!("Operation cancelled.");
        return abort(Cancelled);
    }

    let reporter = InteractiveReporter::new();

    if quiesces && !quiesce(provider.as_ref(), &reporter, &standing, yes.value).await? {
        return abort(Cancelled);
    }

    let context = provider.kube_context().await?;

    let step = reporter.step(&format!("Replacing the {asset} configmap"));
    if let Err(error) = replace_requirements(&context, asset, &contents).await {
        step.fail();
        return Err(error);
    }
    step.done(&format!("Replaced the {asset} configmap"));

    provider.create_runtimes(&reporter).await?;

    if quiesces {
        restore(provider.as_ref(), &reporter, &standing).await?;
    }

    Ok(())
}

fn read_requirements(source: &Path) -> CommandResult<String> {
    let contents = std::fs::read_to_string(source).map_err(|error| {
        TdError::from(CliError::FileReadFailed {
            file: source.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    Ok(contents)
}

fn validate_requirements(source: &Path, contents: &str) -> CommandResult {
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = match line.split_once(" #") {
            Some((requirement, _)) => requirement.trim(),
            None => line,
        };
        if line.starts_with(REQUIREMENTS_OPTION_PREFIX) {
            continue;
        }
        if let Err(error) = Requirement::<VerbatimUrl>::from_str(line) {
            return Err(TdError::from(CliError::RequirementsInvalid {
                file: source.display().to_string(),
                line: index + 1,
                cause: error.to_string(),
            })
            .into());
        }
    }
    Ok(())
}

async fn replace_requirements(context: &KubeContext, asset: &str, contents: &str) -> CommandResult {
    let configmap_name = to_kube_resource_name(
        KPROFILE_ASSETS_FAMILY.scope,
        id_from_file(asset),
        KPROFILE_ASSETS_FAMILY.kind,
        asset,
    )
    .map_err(TdError::from)?;

    let mut configmap_data = BTreeMap::new();
    configmap_data.insert(asset.to_owned(), contents.to_owned());
    replace_configmap(
        context,
        &configmap_name,
        configmap_data,
        BTreeMap::new(),
        Some(KPROFILE_ASSETS_FAMILY.kind),
        None,
    )
    .await
    .map_err(TdError::from)?;
    Ok(())
}

pub(super) struct Standing {
    cluster: bool,
    instance: bool,
}

impl Standing {
    async fn observe(provider: &dyn Provider) -> Self {
        Self {
            cluster: provider.cluster_alive().await,
            instance: provider.instance_alive().await,
        }
    }

    fn running() -> Self {
        Self {
            cluster: true,
            instance: true,
        }
    }
}

async fn quiesce(
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    standing: &Standing,
    yes: bool,
) -> CommandResult<bool> {
    if standing.instance {
        info!("The instance is running and holds the virtual environments in use.");
        info!("It will be stopped so the virtual environments can be rebuilt.");
        if !confirm_at_leisure(yes, "Stop the instance?")? {
            info!("Operation cancelled. The instance stays running.");
            return Ok(false);
        }
        provider.ensure_stoppable_service().await?;
        provider.stop_service(reporter).await?;
    }
    provider.start_cluster(reporter).await?;
    Ok(true)
}

async fn restore(
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    standing: &Standing,
) -> CommandResult {
    if standing.instance {
        provider.ensure_startable_service().await?;
        provider.start_service(reporter).await?;
        report::component::service::started_service(&provider.view());
    } else if !standing.cluster {
        provider.stop_cluster(reporter).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
