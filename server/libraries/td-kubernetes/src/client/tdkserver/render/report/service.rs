//
// Copyright 2026 Tabs Data Inc.
//

use tracing::info;

use crate::client::tdkserver::render::report::common::render_session_lines;
use crate::runtime::provider::summary::Summary;

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
        info!("  export KUBECONFIG={}", kubeconfig.display());
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
            info!("Restarted tabsdata instance '{name}' at {}.", path.display());
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
