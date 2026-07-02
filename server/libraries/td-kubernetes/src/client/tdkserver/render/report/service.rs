//
// Copyright 2026 Tabs Data Inc.
//

use std::path::Path;

use tracing::{info, warn};

#[cfg(feature = "enterprise")]
use crate::client::registry::ag_venv_activate_file;
use crate::client::registry::{InstanceSettings, td_venv_activate_file};
use crate::client::tdkserver::render::preview::render_summary_lines;
use crate::client::tdkserver::render::report::common::{
    activation_command, kubeconfig_export_command, render_session_lines,
};
use crate::runtime::provider::summary::Summary;

const RUNNING_STATUS: &str = "running";
const STOPPED_STATUS: &str = "stopped";
const UNKNOWN_STATUS: &str = "unknown";

pub fn instance_info(settings: &InstanceSettings, folder: &Path, alive: Option<bool>) {
    let resolved = &settings.cluster.resolved;
    let summary = Summary {
        provider: settings.provider,
        instance_name: Some(settings.name.clone()),
        instance_path: Some(folder.to_path_buf()),
        kubeconfig_path: resolved.kubeconfig.clone(),
        context: resolved.context.clone(),
        cluster_url: resolved.url.clone(),
        cluster_name: resolved.cluster.clone(),
        namespace: resolved.namespace.clone(),
    };

    let status = match alive {
        Some(true) => RUNNING_STATUS,
        Some(false) => STOPPED_STATUS,
        None => UNKNOWN_STATUS,
    };

    info!("Tabsdata instance '{}'.", settings.name);
    info!("  - state........: {}", settings.state.as_str());
    info!("  - status.......: {status}");
    info!("  - locality.....: {}", settings.locality.as_str());
    render_summary_lines(&summary);
    info!("Assigned ports:");
    for entry in &settings.ports {
        info!("  - {}: {}", entry.caption, entry.port);
    }

    if let Ok(td_activate) = td_venv_activate_file(&settings.name) {
        info!("To activate the environments:");
        info!("  td: {}", activation_command(&td_activate));
        #[cfg(feature = "enterprise")]
        if let Ok(ag_activate) = ag_venv_activate_file(&settings.name) {
            info!("  ai: {}", activation_command(&ag_activate));
        }
    }

    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("To use kubectl against this cluster, run:");
        info!("  {}", kubeconfig_export_command(kubeconfig));
    }
}

pub fn started_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Started tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("Started tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Started tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Started tabsdata instance.");
    }
    render_session_lines(summary);
    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("To use kubectl against this cluster, run:");
        info!("  {}", kubeconfig_export_command(kubeconfig));
    }
}

pub fn stopped_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Stopped tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("Stopped tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Stopped tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Stopped tabsdata instance.");
    }
    render_session_lines(summary);
}

pub fn restarted_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!(
                "Restarted tabsdata instance '{name}' at {}.",
                path.display()
            );
        } else {
            info!("Restarted tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Restarted tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Restarted tabsdata instance.");
    }
    render_session_lines(summary);
}

pub fn initialized_service(settings: &InstanceSettings) {
    info!("Initialized tabsdata instance '{}'.", settings.name);
    info!("  - state........: {}", settings.state.as_str());
    let resolved = &settings.cluster.resolved;
    let summary = Summary {
        provider: settings.provider,
        instance_name: Some(settings.name.clone()),
        instance_path: None,
        kubeconfig_path: resolved.kubeconfig.clone(),
        context: resolved.context.clone(),
        cluster_url: resolved.url.clone(),
        cluster_name: resolved.cluster.clone(),
        namespace: resolved.namespace.clone(),
    };
    render_summary_lines(&summary);
    warn!(
        "Inspect and edit the config in the instance's init folder, then run \
         'tdkserver create --instance {}' to create the instance.",
        settings.name
    );
}
