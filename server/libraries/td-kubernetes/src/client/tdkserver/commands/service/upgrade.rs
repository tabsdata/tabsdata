//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;
use td_common::status::ExitStatus::Cancelled;
use tracing::info;

use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{instance_edit_folder, instance_init_folder};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::confirm;
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandResult, abort};
use crate::kube::context::KubeContext;
use crate::runtime::instance::upgrade::hop;
use crate::runtime::instance::upgrade::latch;
use crate::runtime::instance::upgrade::lock::UpgradeLock;
use crate::runtime::instance::upgrade::preflight::{Preflight, Standing, inspect};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{ClusterProvider, Provider, provider_for_instance_mutating};

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
        let step = self.current.get() + 1;
        self.current.set(step);
        info!("Step {step} of {} - {caption}", self.total);
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
    } = arguments;

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let apply = apply.unwrap_or(false);
    let unlock = unlock.unwrap_or(false);
    let verbose = globals.verbose;

    let provider = provider_for_instance_mutating(instance.value, verbose, None).await?;
    let summary = provider.summary();
    let instance_name = summary.instance_name.clone().unwrap_or_default();
    let reporter = InteractiveReporter::new();
    let kminus = provider.cluster_provider() == ClusterProvider::Kminus;
    let cluster_start = kminus && !provider.cluster_alive().await;
    let phases = Phases::new(cluster_start, apply);

    if !instance_confirmation_phase(
        &phases,
        &summary,
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
    summary: &Summary,
    in_settings: bool,
    details: bool,
    yes: bool,
) -> CommandResult<bool> {
    phases.announce("Instance confirmation");
    if details {
        let markers = instance_markers(summary, in_settings);
        preview::upgrade_service(summary, &markers);
    }
    let accepted = confirm(yes, "Proceed with the upgrade of this instance?")?;
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
    if !confirm(yes, "Start the cluster?")? {
        info!("Operation cancelled.");
        return Ok(false);
    }
    provider.start_cluster(reporter).await?;
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
        "  Upgrade steps....: {} ({} requiring actions)",
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
    if unlock && preflight.lock.as_ref().is_some_and(UpgradeLock::held) {
        info!("'--unlock' will take over the upgrade lock shown above.");
    }
    if apply {
        info!("'--apply' will start the instance once the upgrade completes.");
    }
    let accepted = confirm(yes, "Proceed with the upgrade?")?;
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
    if !confirm(yes, "Stop the instance and hold it for the upgrade?")? {
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
) -> CommandResult<bool> {
    phases.announce("Instance upgrade");
    info!(
        "  Upgrade steps....: {} ({} requiring actions)",
        preflight.hops.len(),
        preflight.upgraders()
    );
    info!("The instance resources will be brought to each version in turn.");
    info!("The python virtual environments will be rebuilt.");
    info!("The database will be brought up to date.");
    for folder in editable_folders(instance)? {
        info!("The editable folder '{folder}' will be deleted.");
    }
    if !confirm(yes, "Upgrade the resources of this instance?")? {
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
    provider.upgrade_runtimes(reporter).await?;
    provider.upgrade_database(reporter).await?;

    latch::finalise(context, instance, lock, &preflight.product_version).await?;
    info!(
        "Instance '{instance}' is upgraded to version '{}'.",
        preflight.product_version
    );
    Ok(true)
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
    if !confirm(yes, "Start the instance?")? {
        info!("Operation cancelled. The instance stays stopped.");
        return Ok(false);
    }
    provider.start_service(reporter).await?;
    report::component::service::started_service(&provider.summary());
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
