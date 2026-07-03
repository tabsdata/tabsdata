//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::{BTreeMap, HashMap};

use crate::base::constants::SECRET_FAMILY_CONFIG;
use crate::base::reporter::{InteractiveReporter, Reporter};
#[cfg(feature = "enterprise")]
use crate::client::registry::ag_venv_activate_file;
use crate::client::registry::{
    InstanceSettings, InstanceState, instance_init_input_folder, instance_init_stage_folder,
    td_venv_activate_file,
};
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::client::tdkserver::commands::service::{
    instance_markers, resolved_instance, resolved_preview, resolved_yes,
};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::client::tdkserver::render::preview;
use crate::client::tdkserver::render::report::helper::common::activation_command;
use crate::core::system::exit::{CommandResult, abort};
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
use td_common::status::ExitStatus::Cancelled;
use td_error::TdError;
use tracing::info;
use tracing::warn;

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
        name = "preview",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before creating the instance. \
                     Defaults to 'true'. Pass '--preview false' to skip the summary."
    )]
    pub preview: Option<bool>,

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
        preview,
        yes,
    } = arguments;

    let pins = PinContext::pins();
    let instance = resolved_instance(instance, &pins);
    let preview = resolved_preview(preview, &pins);
    let yes = resolved_yes(yes, &pins);
    let verbose = globals.verbose;

    create_instance(
        instance.value,
        instance.pinned,
        port,
        preview.value,
        yes.value,
        verbose,
        &HashMap::new(),
    )
    .await
}

pub async fn create_instance(
    instance: Option<String>,
    instance_pinned: bool,
    port: u16,
    preview: bool,
    yes: bool,
    verbose: bool,
    secret_overrides: &HashMap<String, String>,
) -> CommandResult {
    let instance = instance.unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());

    let mut settings = InstanceSettings::load_required(&instance, &[InstanceState::Initialized])?;

    let init = instance_init_input_folder(&settings.name)?;
    let stage = instance_init_stage_folder(&settings.name)?;

    let profile = load_profile(settings.provider, verbose)?;

    let assigned = assign_ports(port);

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
        verbose,
        kminus_api_port(&assigned),
    )
    .await?;

    if preview {
        let markers = instance_markers(&provider.summary(), instance_pinned);
        preview::create_service(&provider.summary(), &markers);
        report_ports(&assigned);
    }
    if !yes && !prompt_yes_no("Proceed with the creation?") {
        info!("Operation cancelled.");
        return abort(Cancelled);
    }

    if !preview {
        report_ports(&assigned);
    }
    settings.ports = assigned.clone();
    settings.save()?;

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

    settings.cluster = provider.refresh_cluster_views(settings.cluster)?;
    settings.state = InstanceState::Created;
    settings.save()?;

    report_venv_activation(&settings.name)?;

    Ok(())
}

fn report_venv_activation(name: &str) -> CommandResult {
    let mut message = String::from("To activate the created environments:");

    let td_activate = td_venv_activate_file(name)?;
    message.push_str(&format!("\n  td: {}", activation_command(&td_activate)));

    #[cfg(feature = "enterprise")]
    {
        let ag_activate = ag_venv_activate_file(name)?;
        message.push_str(&format!("\n  ai: {}", activation_command(&ag_activate)));
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
