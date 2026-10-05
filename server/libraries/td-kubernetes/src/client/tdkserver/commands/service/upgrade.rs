//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use clap_derive::Args;
use colored::Colorize;
use td_common::status::ExitStatus::Cancelled;
use td_error::TdError;
use tracing::info;

use crate::base::constants::{ENV_TD_KUBE_UV_MODE, KPROFILE_CONFIGS_FAMILY, UvMode};
use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{InstanceSettings, instance_edit_folder, instance_init_folder};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::{Choice, choose_at_leisure, confirm_at_leisure};
use crate::client::tdkserver::render::view::InstanceView;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort};
use crate::cwarn;
use crate::kube::api::entity::configmap::{get_configmap, replace_configmap};
use crate::kube::context::KubeContext;
use crate::runtime::instance::index;
use crate::runtime::instance::tabsdata::{TABSDATA_FILE, TabsdataConfig};
use crate::runtime::instance::upgrade::hop;
use crate::runtime::instance::upgrade::latch;
use crate::runtime::instance::upgrade::lock::UpgradeLock;
use crate::runtime::instance::upgrade::preflight::{Preflight, Standing, inspect};
use crate::runtime::provider::{ClusterProvider, Provider, provider_for_instance_upgrading};
use crate::runtime::secrets::settler::pending_secret_keys;

struct Phases {
    total: usize,
    current: std::cell::Cell<usize>,
}

impl Phases {
    fn new(cluster_start: bool, apply: bool) -> Self {
        let mut total = 4;
        if cluster_start {
            total += 1;
        }
        if apply {
            total += 1;
        }
        Self {
            total,
            current: std::cell::Cell::new(0),
        }
    }

    fn announce(&self, caption: &str) {
        let phase = self.current.get() + 1;
        self.current.set(phase);
        println!(
            "{}",
            format!("Phase {phase} of {} - {caption}", self.total)
                .bold()
                .truecolor(0x56, 0xD4, 0xDD)
        );
    }
}

#[derive(Debug, Clone, Args)]
pub struct UpgradeArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to upgrade. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "apply",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 70,
        long_help = "Start the instance once the upgrade completes. Defaults to 'false'."
    )]
    pub apply: Option<bool>,

    #[arg(
        long,
        name = "unlock",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 80,
        long_help = "Take over the upgrade lock of an interrupted upgrade and carry on. The \
                     version the instance is being upgraded to is kept, so only a tabsdata of \
                     that version can resume it. Does nothing when no lock is held."
    )]
    pub unlock: Option<bool>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before the upgrade. Defaults to \
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
        long_help = "Accept every confirmation prompt and proceed immediately."
    )]
    pub yes: Option<bool>,

    #[arg(
        long = "uv-index",
        name = "uv-index",
        required = false,
        value_name = "URI",
        display_order = 150,
        long_help = "Python package index the rebuilt virtual environments resolve from. The \
                     URI is set as the 'UV_INDEX' global environment variable of the instance, \
                     which every container of every pod receives, and it takes precedence over \
                     the 'UV_INDEX' and 'PIP_INDEX_URL' variables of this machine.\n\n\
                     Only valid for a 'kminus' instance with 'TD_KUBE_UV_MODE' unset, because \
                     only then are the packages resolved from this machine's environment.\n\n\
                     Example:\n  \
                     tdk upgrade --uv-index http://localhost:8000/+simple"
    )]
    pub uv_index: Option<String>,
}

pub async fn command_upgrade(
    arguments: UpgradeArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let UpgradeArguments {
        instance,
        apply,
        unlock,
        details,
        yes,
        uv_index,
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let apply = apply.unwrap_or(false);
    let unlock = unlock.unwrap_or(false);
    let verbose = globals.verbose;

    let provider = provider_for_instance_upgrading(instance.value, verbose).await?;
    let view = provider.view();
    let instance_name = view.instance_name.clone().unwrap_or_default();
    let reporter = InteractiveReporter::new();
    let kminus = provider.cluster_provider() == ClusterProvider::Kminus;
    let cluster_start = kminus && !provider.cluster_alive().await;
    let phases = Phases::new(cluster_start, apply);

    if !instance_confirmation_phase(
        &phases,
        &view,
        instance.in_settings,
        details.value,
        yes.value,
    )? {
        return abort(Cancelled);
    }

    if cluster_start
        && !cluster_start_phase(&phases, provider.as_ref(), &reporter, yes.value).await?
    {
        return abort(Cancelled);
    }

    let context = provider.kube_context().await?;
    let preflight = inspect(&context, &instance_name, verbose).await?;

    if !upgrade_viability_phase(&phases, &preflight, unlock, apply, yes.value)? {
        return abort(Cancelled);
    }
    let lock = hold_lock(&context, &instance_name, &preflight, unlock).await?;

    if !instance_shutdown_phase(
        &phases,
        provider.as_ref(),
        &reporter,
        &context,
        &preflight,
        &lock,
        kminus,
        yes.value,
    )
    .await?
    {
        return abort(Cancelled);
    }

    if !instance_upgrade_phase(
        &phases,
        provider.as_ref(),
        &reporter,
        &context,
        &instance_name,
        &preflight,
        &lock,
        verbose,
        yes.value,
        uv_index.as_deref(),
    )
    .await?
    {
        return abort(Cancelled);
    }

    if apply {
        instance_restart_phase(
            &phases,
            provider.as_ref(),
            &reporter,
            &preflight.product_version,
            yes.value,
        )
        .await?;
    }

    Ok(())
}

fn instance_confirmation_phase(
    phases: &Phases,
    view: &InstanceView,
    in_settings: bool,
    details: bool,
    yes: bool,
) -> CommandResult<bool> {
    phases.announce("Instance confirmation");
    if details {
        let markers = instance_markers(view, in_settings);
        preview::upgrade_service(view, &markers);
    }
    let accepted = confirm_at_leisure(yes, "Proceed with the upgrade of this instance?")?;
    if !accepted {
        info!("Operation cancelled.");
    }
    Ok(accepted)
}

async fn cluster_start_phase(
    phases: &Phases,
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    yes: bool,
) -> CommandResult<bool> {
    phases.announce("Cluster start");
    info!("The cluster of this instance is stopped.");
    info!("It will be started so the instance can be inspected and upgraded.");
    info!("The instance itself stays stopped.");
    if !confirm_at_leisure(yes, "Start the cluster?")? {
        info!("Operation cancelled.");
        return Ok(false);
    }
    provider.start_cluster(reporter).await?;
    Ok(true)
}

async fn settle_index(
    provider: &dyn Provider,
    context: &KubeContext,
    uv_index: Option<&str>,
    yes: bool,
) -> CommandResult<bool> {
    let uv_mode = UvMode::from_environment().map_err(TdError::from)?;
    if !index::governed(provider.cluster_provider(), uv_mode) {
        if uv_index.is_some() {
            return Err(TdError::from(CliError::UvIndexNotGoverned {
                variable: ENV_TD_KUBE_UV_MODE.to_owned(),
                provider: provider.cluster_provider().as_str().to_owned(),
                mode: uv_mode.as_str().to_owned(),
            })
            .into());
        }
        return Ok(true);
    }

    let configmap = to_kube_resource_name(
        KPROFILE_CONFIGS_FAMILY.scope,
        id_from_file(TABSDATA_FILE),
        KPROFILE_CONFIGS_FAMILY.kind,
        TABSDATA_FILE,
    )
    .map_err(TdError::from)?;
    let Some(mut data) = get_configmap(context, &configmap).await? else {
        return Err(
            ProviderError::ConfigmapEntryMissing(configmap, TABSDATA_FILE.to_owned()).into(),
        );
    };
    let Some(yaml) = data.remove(TABSDATA_FILE) else {
        return Err(
            ProviderError::ConfigmapEntryMissing(configmap, TABSDATA_FILE.to_owned()).into(),
        );
    };
    let mut config = TabsdataConfig::parse(&yaml).map_err(|error| {
        TdError::from(CliError::FileParseFailed {
            file: TABSDATA_FILE.to_owned(),
            cause: error.to_string(),
        })
    })?;

    let plan = index::resolve(uv_index, index::remote_index(&config));
    info!(
        "  Python package index of this machine.: {}",
        plan.obtain_local()
    );
    info!(
        "  Python package index of the instance.: {}",
        plan.obtain_remote()
    );
    if plan.same() {
        info!(
            "The instance will use the Python package index: {}",
            plan.obtain_remote()
        );
        return Ok(true);
    }

    if plan.none() {
        cwarn!(
            "The Python package index '{}' of this instance will be cleared, because \
             neither '--uv-index' nor the index environment variables are set here.",
            plan.obtain_remote()
        );
    }
    let choice = choose_at_leisure(
        yes,
        "The indexes differ. Continue with the instance one, cancel, or change it?",
        Choice::Change,
    )?;
    match choice {
        Choice::Cancel => return Ok(false),
        Choice::Continue => {
            info!(
                "The instance will use the Python package index: {}",
                plan.obtain_remote()
            );
            return Ok(true);
        }
        Choice::Change => {}
    }

    index::apply(&mut config, plan.local.as_deref());
    let updated = serde_yaml::to_string(&config).map_err(|error| {
        TdError::from(CliError::FileSerializeFailed {
            file: TABSDATA_FILE.to_owned(),
            cause: error.to_string(),
        })
    })?;
    let mut written = BTreeMap::new();
    written.insert(TABSDATA_FILE.to_owned(), updated);
    replace_configmap(
        context,
        &configmap,
        written,
        BTreeMap::new(),
        BTreeMap::new(),
        Some(KPROFILE_CONFIGS_FAMILY.kind),
        None,
    )
    .await
    .map_err(TdError::from)?;
    info!(
        "The instance will use the Python package index: {}",
        plan.obtain_local()
    );
    Ok(true)
}

fn upgrade_viability_phase(
    phases: &Phases,
    preflight: &Preflight,
    unlock: bool,
    apply: bool,
    yes: bool,
) -> CommandResult<bool> {
    phases.announce("Upgrade viability");
    info!("  Instance version.: {}", preflight.instance_version);
    info!("  Tabsdata version.: {}", preflight.product_version);
    report_lock(preflight.lock.as_ref());
    info!(
        "  Version steps...: {} ({} requiring actions)",
        preflight.hops.len(),
        preflight.upgraders()
    );
    for hop in &preflight.hops {
        let carries = if hop.upgrader {
            "Actions required"
        } else {
            "No actions required"
        };
        info!("    - {} -> {}: {carries}", hop.from, hop.to);
    }

    if let Standing::Upgrading { held: true, .. } = preflight.standing
        && !unlock
    {
        info!("Another actor holds the upgrade lock of this instance.");
        info!("Pass '--unlock' to take it over if you know that upgrade is not running.");
        return Ok(false);
    }
    if preflight.up_to_date() {
        info!(
            "Instance '{}' is already at version '{}'; nothing to upgrade.",
            preflight.instance, preflight.instance_version
        );
        return Ok(false);
    }
    if preflight.interrupted() && preflight.hops.is_empty() {
        info!(
            "A previous upgrade to '{}' left this instance unfinished.",
            preflight.product_version
        );
        info!("Its resources are upgraded; the runtimes and the database are not.");
        info!("This run will complete them.");
    }
    if unlock && preflight.lock.as_ref().is_some_and(UpgradeLock::held) {
        info!("'--unlock' will take over the upgrade lock shown above.");
    }
    if apply {
        info!("'--apply' will start the instance once the upgrade completes.");
    }
    let accepted = confirm_at_leisure(yes, "Proceed with the upgrade?")?;
    if !accepted {
        info!("Operation cancelled.");
    }
    Ok(accepted)
}

async fn hold_lock(
    context: &KubeContext,
    instance: &str,
    preflight: &Preflight,
    unlock: bool,
) -> CommandResult<UpgradeLock> {
    if unlock && let Some(cleared) = latch::unlock(context, instance).await? {
        info!(
            "Took over the upgrade lock '{}' of instance '{instance}'.",
            cleared.id.unwrap_or_default()
        );
    }
    let lock = latch::acquire(context, instance, &preflight.product_version).await?;
    info!(
        "Holding the upgrade lock '{}' for the upgrade to '{}'.",
        lock.id.clone().unwrap_or_default(),
        lock.target
    );
    Ok(lock)
}

#[allow(clippy::too_many_arguments)]
async fn instance_shutdown_phase(
    phases: &Phases,
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    context: &KubeContext,
    preflight: &Preflight,
    lock: &UpgradeLock,
    kminus: bool,
    yes: bool,
) -> CommandResult<bool> {
    phases.announce("Instance shutdown");
    if provider.service_alive().await {
        info!("The instance is running and will be stopped.");
    } else {
        info!("The instance is already stopped.");
    }
    if kminus {
        info!("The cluster stays running so the upgrade can reach the instance.");
    }
    info!(
        "The upgrade will take the instance from '{}' to '{}'.",
        preflight.instance_version, preflight.product_version
    );
    if !confirm_at_leisure(yes, "Stop the instance and hold it for the upgrade?")? {
        info!("Operation cancelled.");
        return Ok(false);
    }
    latch::verify(context, &preflight.instance, lock).await?;
    provider.stop_service(reporter).await?;
    if kminus {
        provider.start_cluster(reporter).await?;
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn instance_upgrade_phase(
    phases: &Phases,
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    context: &KubeContext,
    instance: &str,
    preflight: &Preflight,
    lock: &UpgradeLock,
    verbose: bool,
    yes: bool,
    uv_index: Option<&str>,
) -> CommandResult<bool> {
    phases.announce("Instance upgrade");
    info!(
        "  Version steps...: {} ({} requiring actions)",
        preflight.hops.len(),
        preflight.upgraders()
    );
    info!("The instance resources will be brought to each version in turn.");
    info!("The python virtual environments will be rebuilt.");
    info!("The database will be brought up to date.");
    for folder in editable_folders(instance)? {
        info!("The editable folder '{folder}' will be deleted.");
    }
    if !confirm_at_leisure(yes, "Upgrade the resources of this instance?")? {
        info!("Operation cancelled.");
        return Ok(false);
    }

    for folder in hop::discard_editable_folders(instance)? {
        info!("Deleted the editable folder '{folder}'.");
    }

    let walked = hop::upgrade_resources(
        provider,
        reporter,
        context,
        instance,
        &preflight.hops,
        lock,
        verbose,
    )
    .await?;
    info!(
        "Upgraded {} resource(s); {} retained.",
        walked.applied, walked.retained
    );

    latch::verify(context, instance, lock).await?;
    if !settle_index(provider, context, uv_index, yes).await? {
        info!("Operation cancelled.");
        return Ok(false);
    }
    provider.upgrade_runtimes(reporter).await?;
    provider.upgrade_database(reporter).await?;

    latch::finalise(context, instance, lock, &preflight.product_version).await?;

    let mut settings = InstanceSettings::load(instance)?;
    settings.version = preflight.product_version.clone();
    settings.save()?;

    info!(
        "Instance '{instance}' is upgraded to version '{}'.",
        preflight.product_version
    );
    report_pending_secrets(context, instance).await?;
    Ok(true)
}

async fn report_pending_secrets(context: &KubeContext, instance: &str) -> CommandResult {
    let pending = pending_secret_keys(context).await?;
    if pending.is_empty() {
        return Ok(());
    }
    cwarn!(
        "This version needs {} configuration secret(s) that have no value yet. Only \
         you can provide them, and the instance will not start until you do:",
        pending.iter().map(|entry| entry.keys.len()).sum::<usize>()
    );
    for entry in &pending {
        info!("  - {}: {}", entry.config, entry.keys.join(", "));
    }
    info!("Download the configurations, fill the values in, and upload them back:");
    info!("  tdkserver config get --instance {instance}");
    info!("  tdkserver config put --instance {instance}");
    Ok(())
}

async fn instance_restart_phase(
    phases: &Phases,
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    version: &str,
    yes: bool,
) -> CommandResult<bool> {
    phases.announce("Instance restart");
    info!("The instance is upgraded to '{version}' and will be started.");
    if !confirm_at_leisure(yes, "Start the instance?")? {
        info!("Operation cancelled. The instance stays stopped.");
        return Ok(false);
    }
    provider.start_service(reporter).await?;
    report::component::service::started_service(&provider.view());
    Ok(true)
}

fn editable_folders(instance: &str) -> CommandResult<Vec<String>> {
    let mut present = Vec::new();
    for folder in [
        instance_init_folder(instance)?,
        instance_edit_folder(instance)?,
    ] {
        if folder.is_dir() {
            present.push(folder.display().to_string());
        }
    }
    Ok(present)
}

fn report_lock(lock: Option<&UpgradeLock>) {
    match lock {
        None => info!("  Upgrade lock.....: None"),
        Some(lock) if lock.held() => {
            info!("  Upgrade lock.....: Held");
            info!("    - Target.......: {}", lock.target);
            info!(
                "    - Identifier...: {}",
                lock.id.clone().unwrap_or_default()
            );
            info!(
                "    - Taken at.....: {}",
                lock.timestamp
                    .map(|when| when.to_rfc3339())
                    .unwrap_or_default()
            );
        }
        Some(lock) => {
            info!("  Upgrade lock.....: Released, upgrade unfinished");
            info!("    - Target.......: {}", lock.target);
        }
    }
}
