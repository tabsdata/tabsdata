//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::{BTreeMap, HashMap};
use std::fs;

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;
use td_error::TdError;
use tracing::info;

use crate::base::constants::{KPROFILE_CONFIGS_FAMILY, SECRET_FAMILY_CONFIG};
use crate::base::error::cli::CliError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{
    InstanceSettings, instance_edit_input_folder, instance_edit_stage_folder,
};
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::restart::restart_service;
use crate::client::tdkserver::commands::service::{
    resolved_instance, resolved_details, resolved_yes,
};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::core::system::exit::{CommandResult, abort};
use crate::kube::api::configmap::replace_configmap;
use crate::kube::api::secret::{get_secret, upsert_secret};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::resolver::fusion::{MergedConfig, merge};
use crate::runtime::instance::ports::apply_ports;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::Provider;
use crate::runtime::provider::provider_for_instance;
use crate::runtime::secrets::source::to_secret_name;

const SCHEMA_EXTENSION: &str = ".yamls";

#[derive(Debug, Clone, Args)]
pub struct PutArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose configmaps are replaced. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "apply",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 70,
        long_help = "Restart the instance after replacing the configmaps. Defaults to 'false'."
    )]
    pub apply: Option<bool>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before replacing the configmaps. \
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

pub async fn command_put(arguments: PutArguments, globals: &GlobalArguments) -> CommandResult {
    let PutArguments {
        instance,
        apply,
        details,
        yes,
    } = arguments;

    let pins = PinContext::pins();
    let instance = resolved_instance(instance, &pins);
    let details = resolved_details(details, &pins);
    let yes = resolved_yes(yes, &pins);
    let apply = apply.unwrap_or(false);
    let verbose = globals.verbose;

    let provider = provider_for_instance(instance.value, verbose).await?;
    let name = provider.summary().instance_name.unwrap_or_default();

    let profile = load_profile(provider.cluster_provider(), verbose)?;
    let settings = InstanceSettings::load(&name)?;
    let mut system = provider.system_values()?;
    apply_ports(&mut system, &settings.ports);
    let context = provider.kube_context().await?;

    let input = instance_edit_input_folder(&name)?;
    let server_secrets = read_server_secrets(&context, &profile).await?;

    let merged = merge(&profile, &system, &input, &server_secrets).map_err(TdError::from)?;

    if details.value {
        info!("About to replace configmaps for tabsdata instance '{name}':");
        for config in &merged {
            info!("  - {}", config.file);
        }
        if apply {
            info!("The instance will be restarted after the replacement.");
        }
    }
    if !yes.value && !prompt_yes_no("Proceed with the replacement?") {
        info!("Operation cancelled.");
        return abort(Cancelled);
    }

    write_stage(&name, &merged)?;

    let reporter = InteractiveReporter::new();
    let step = reporter.step("Replacing tabsdata configmaps and secrets");
    for config in &merged {
        if let Err(error) = replace_config(&context, config).await {
            step.fail();
            return Err(error);
        }
    }
    step.done(&format!("Replaced {} tabsdata configmaps", merged.len()));

    if apply {
        restart(provider.as_ref(), &reporter).await?;
    }

    Ok(())
}

async fn read_server_secrets(
    context: &KubeContext,
    profile: &Profile,
) -> CommandResult<HashMap<String, HashMap<String, String>>> {
    let mut server_secrets = HashMap::new();
    for file in profile.configs.keys() {
        if file.ends_with(SCHEMA_EXTENSION) {
            continue;
        }
        let secret_name = to_secret_name(file).map_err(TdError::from)?;
        let Some(data) = get_secret(context, &secret_name)
            .await
            .map_err(TdError::from)?
        else {
            continue;
        };
        let mut values = HashMap::new();
        for (key, value) in data {
            if let Ok(text) = String::from_utf8(value) {
                values.insert(key, text);
            }
        }
        if !values.is_empty() {
            server_secrets.insert(file.clone(), values);
        }
    }
    Ok(server_secrets)
}

fn write_stage(name: &str, merged: &[MergedConfig]) -> CommandResult {
    let stage = instance_edit_stage_folder(name)?;
    let configs_folder = stage.join(KPROFILE_CONFIGS_FAMILY.folder);
    fs::create_dir_all(&configs_folder).map_err(|error| {
        TdError::from(CliError::FolderCreateFailed {
            folder: configs_folder.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    for config in merged {
        let target = configs_folder.join(&config.file);
        fs::write(&target, &config.rendered).map_err(|error| {
            TdError::from(CliError::FileWriteFailed {
                file: target.display().to_string(),
                cause: error.to_string(),
            })
        })?;
    }
    Ok(())
}

async fn replace_config(context: &KubeContext, config: &MergedConfig) -> CommandResult {
    let id = id_from_file(&config.file);
    let configmap_name = to_kube_resource_name(
        KPROFILE_CONFIGS_FAMILY.scope,
        id,
        KPROFILE_CONFIGS_FAMILY.kind,
        &config.file,
    )
    .map_err(TdError::from)?;

    let mut configmap_data = BTreeMap::new();
    configmap_data.insert(config.file.clone(), config.rendered.clone());
    replace_configmap(
        context,
        &configmap_name,
        configmap_data,
        BTreeMap::new(),
        Some(KPROFILE_CONFIGS_FAMILY.kind),
    )
    .await
    .map_err(TdError::from)?;

    let secret_name = to_secret_name(&config.file).map_err(TdError::from)?;
    let secret_data: BTreeMap<String, String> = config
        .secrets
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect();
    upsert_secret(
        context,
        &secret_name,
        secret_data,
        BTreeMap::new(),
        None,
        Some(SECRET_FAMILY_CONFIG),
    )
    .await
    .map_err(TdError::from)?;

    Ok(())
}

async fn restart(provider: &dyn Provider, reporter: &dyn Reporter) -> CommandResult {
    let not_dead = !provider.service_dead().await;
    if not_dead {
        provider.ensure_stoppable_service().await?;
    }
    provider.ensure_startable_service().await?;
    restart_service(provider, reporter, not_dead).await
}
