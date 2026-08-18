//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::base::constants::SECRET_FAMILY_CONFIG;
use crate::base::reporter::{InteractiveReporter, Reporter};
#[cfg(feature = "enterprise")]
use crate::client::registry::ag_venv_activate_file;
use crate::client::registry::{
    InstanceSettings, InstanceState, fn_venv_activate_file, instance_init_input_folder,
    instance_init_stage_folder, td_venv_activate_file, venvs_root,
};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::{GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::service::{
    instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::preview;
use crate::client::tdkserver::render::report::helper::common::activation_command;
use crate::client::tdkserver::widgets::show_hint;
use crate::client::vault::Vault;
use crate::core::system::exit::{CommandResult, abort};
use crate::kube::api::secret::upsert_secret;
use crate::kube::api::{ensure_cluster_reachable, ensure_namespace_exists};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::resolver::fusion;
use crate::runtime::gateway::resolver::fusion::FusedSecrets;
use crate::runtime::instance::launcher::ensure_ports_unclaimed;
use crate::runtime::instance::ports::{
    AssignedPort, DEFAULT_BASE_PORT, NO_PATIENCE, apply_ports, assign_ports, ensure_ports_free,
    kminus_api_port,
};
use crate::runtime::instance::profile::load_profile;
use crate::runtime::instance::verify_instance_version;
use crate::runtime::provider::{ClusterProvider, cluster_arguments_for, provider_for};
use crate::runtime::secrets::source::to_secret_name;
use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::Cancelled;
use td_error::TdError;
use tracing::info;

#[derive(Debug, Clone, Args)]
pub struct CreateArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to create. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

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

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before creating the instance. \
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

pub async fn command_create(
    arguments: CreateArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let CreateArguments {
        instance,
        port,
        details,
        yes,
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let verbose = globals.verbose;

    create_instance(
        instance.value,
        instance.in_settings,
        port,
        details.value,
        yes.value,
        verbose,
        &HashMap::new(),
    )
    .await
}

pub async fn create_instance(
    instance: Option<String>,
    instance_in_settings: bool,
    port: u16,
    details: bool,
    yes: bool,
    verbose: bool,
    secret_overrides: &HashMap<String, String>,
) -> CommandResult {
    let instance = instance.unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());

    let mut settings = InstanceSettings::load_required(&instance, &[InstanceState::Initialized])?;
    verify_instance_version(&settings.name, &settings.version)?;

    let init = instance_init_input_folder(&settings.name)?;
    let stage = instance_init_stage_folder(&settings.name)?;

    let profile = load_profile(settings.provider, verbose)?;

    let assigned = assign_ports(port);
    if let Some(filter) = settings.provider.ports_to_check_on_host() {
        ensure_ports_free(&assigned, filter, NO_PATIENCE)?;
    }

    let cluster = cluster_arguments_for(&settings);
    let provider = provider_for(
        cluster,
        InstanceArguments::new(Some(settings.name.clone())),
        verbose,
        kminus_api_port(&assigned),
        settings.loglevel,
    )
    .await?;

    if settings.provider != ClusterProvider::Kminus {
        let context = provider.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        provider.ensure_creatable_service().await?;
        if let Some(filter) = settings.provider.ports_to_check_on_host() {
            ensure_ports_unclaimed(&context, &assigned, filter).await?;
        }
    }

    if details {
        let markers = instance_markers(&provider.summary(), instance_in_settings);
        preview::create_service(&provider.summary(), &markers);
        report_ports(&assigned);
    }
    if !confirm(yes, "Proceed with the creation?")? {
        info!("Operation cancelled.");
        return abort(Cancelled);
    }

    if !details {
        report_ports(&assigned);
    }
    settings.ports = assigned.clone();
    settings.save()?;

    let mut system = provider.system_values()?;
    let mut vault = Vault::of(&settings.name)?;
    if settings.provider == ClusterProvider::Kminus {
        vault.provision(&profile)?;
    }
    let auto: HashMap<String, String> = vault.secrets().clone().into_iter().collect();

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
    if settings.provider == ClusterProvider::Kminus {
        vault.persist(&fused, secret_overrides)?;
    }

    provider.create_runtimes(&reporter).await?;
    provider.create_images(&reporter).await?;
    provider.create_database(&reporter).await?;

    provider.stop_cluster(&reporter).await?;
    tx.commit();

    settings.cluster = provider.refresh_cluster_views(settings.cluster)?;
    settings.state = InstanceState::Created;
    settings.save()?;

    // if provider.cluster_provider().hosts_runtimes()? {
    //     report_venv_activation(&settings.name)?;
    // }

    Ok(())
}

#[allow(dead_code)]
fn report_venv_activation(name: &str) -> CommandResult {
    let root = venvs_root(name)?;
    let mut message = format!(
        "To activate the created environments:\n  cd {}\n",
        root.display()
    );

    let td_activate = td_venv_activate_file(name)?;
    message.push_str(&format!(
        "\n  td: {}",
        activation_command(&relative_to(&td_activate, &root))
    ));

    #[cfg(feature = "enterprise")]
    {
        let ag_activate = ag_venv_activate_file(name)?;
        message.push_str(&format!(
            "\n  ai: {}",
            activation_command(&relative_to(&ag_activate, &root))
        ));
    }

    let fn_activate = fn_venv_activate_file(name)?;
    message.push_str(&format!(
        "\n  fn: {}",
        activation_command(&relative_to(&fn_activate, &root))
    ));

    show_hint(&message, 80, true);
    Ok(())
}

#[allow(dead_code)]
fn relative_to(activate: &Path, root: &Path) -> PathBuf {
    match activate.strip_prefix(root) {
        Ok(relative) => Path::new(".").join(relative),
        Err(_) => activate.to_path_buf(),
    }
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
