//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::{BTreeMap, HashMap};

#[cfg(feature = "enterprise")]
use crate::base::constants::FOLDER_PYENV_AG;
use crate::base::constants::SECRET_FAMILY_CONFIG;
use crate::base::constants::{FOLDER_PYENV_TD, FOLDER_PYTHON, FOLDER_RUNTIMES};
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{
    InstanceSettings, InstanceState, instance_folder, instance_init_folder, instance_stage_folder,
};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::CommandResult;
use crate::kube::api::secret::upsert_secret;
use crate::kube::context::KubeContext;
use crate::runtime::gateway::resolver::fusion;
use crate::runtime::gateway::resolver::fusion::FusedSecrets;
use crate::runtime::instance::ports::{
    AssignedPort, DEFAULT_BASE_PORT, apply_ports, assign_ports, kminus_api_port,
};
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::provider_for;
use crate::runtime::secrets::source::to_secret_name;
use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_error::TdError;
use tracing::info;
use tracing::warn;

#[derive(Debug, Clone, Args)]
pub struct CreateArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Name of the instance to create. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "port",
        required = false,
        default_value_t = DEFAULT_BASE_PORT,
        long_help = "Base HTTP/HTTPS port. Ports for the instance services are assigned \
                     sequentially starting from this value."
    )]
    pub port: u16,
}

pub async fn command_create(
    arguments: CreateArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let CreateArguments { instance, port } = arguments;
    create_instance(instance, port, &HashMap::new(), globals).await
}

pub async fn create_instance(
    instance: Option<String>,
    port: u16,
    secret_overrides: &HashMap<String, String>,
    globals: &GlobalArguments,
) -> CommandResult {
    let instance = instance.unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());

    let mut settings = InstanceSettings::load_required(&instance, &[InstanceState::Initialized])?;

    let init = instance_init_folder(&settings.name)?;
    let stage = instance_stage_folder(&settings.name)?;

    let profile = load_profile(settings.provider, globals.verbose)?;

    let assigned = assign_ports(port);
    report_ports(&assigned);
    settings.ports = assigned.clone();
    settings.save()?;

    let cluster = ClusterArguments {
        provider: Some(settings.provider),
        context: None,
        kubeconfig: None,
        current: false,
        namespace: None,
    };
    let provider = provider_for(
        cluster,
        InstanceArguments::new(Some(settings.name.clone())),
        globals.verbose,
        kminus_api_port(&assigned),
    )
    .await?;
    let mut system = provider.system_values()?;
    let auto: HashMap<String, String> = HashMap::new();

    apply_ports(&mut system, &assigned);

    let reporter = InteractiveReporter::new();

    let step = reporter.step("Fusing tabsdata instance configuration");
    let fused = match fusion::fuse(&profile, &system, &auto, &init, &stage) {
        Ok(fused) => fused,
        Err(error) => {
            step.fail();
            return Err(TdError::from(error).into());
        }
    };
    step.done("Fused tabsdata instance configuration");

    let tx = provider.create_cluster_hook(&reporter).await?;

    let context = provider.kube_context().await?;
    store_secrets(&context, &fused, secret_overrides, &reporter).await?;

    provider.create_runtimes(&reporter).await?;
    provider.create_images(&reporter).await?;
    provider.create_database(&reporter).await?;

    provider.stop_cluster(&reporter).await?;
    tx.commit();

    settings.state = InstanceState::Created;
    settings.save()?;

    report_venv_activation(&settings.name)?;

    Ok(())
}

fn report_venv_activation(name: &str) -> CommandResult {
    let python = instance_folder(name)?
        .join(FOLDER_RUNTIMES)
        .join(FOLDER_PYTHON);

    let mut message = String::from("To activate the created environments:");

    let td_activate = python.join(FOLDER_PYENV_TD).join("bin").join("activate");
    message.push_str(&format!("\n  td: source {}", td_activate.display()));

    #[cfg(feature = "enterprise")]
    {
        let ag_activate = python.join(FOLDER_PYENV_AG).join("bin").join("activate");
        message.push_str(&format!("\n  ai: source {}", ag_activate.display()));
    }

    warn!("{message}");
    Ok(())
}

async fn store_secrets(
    context: &KubeContext,
    fused: &FusedSecrets,
    overrides: &HashMap<String, String>,
    reporter: &InteractiveReporter,
) -> CommandResult {
    if fused.by_file.is_empty() {
        return Ok(());
    }
    let step = reporter.step("Storing tabsdata secrets");
    for (file, entries) in &fused.by_file {
        let name = to_secret_name(file)?;
        let data: BTreeMap<String, String> = entries
            .iter()
            .map(|entry| {
                let value = entry
                    .category
                    .as_ref()
                    .and_then(|category| overrides.get(category))
                    .cloned()
                    .unwrap_or_else(|| entry.value.clone());
                (entry.key.clone(), value)
            })
            .collect();
        if let Err(error) = upsert_secret(
            context,
            &name,
            data,
            BTreeMap::new(),
            None,
            Some(SECRET_FAMILY_CONFIG),
        )
        .await
        {
            step.fail();
            return Err(TdError::from(error).into());
        }
    }
    step.done("Stored tabsdata secrets");
    Ok(())
}

fn report_ports(assigned: &[AssignedPort]) {
    info!("Assigned ports:");
    for entry in assigned {
        info!("  - {}: {}", entry.caption, entry.port);
    }
}
