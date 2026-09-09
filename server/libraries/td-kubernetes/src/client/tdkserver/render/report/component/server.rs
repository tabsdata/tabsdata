//
// Copyright 2026 Tabsdata Inc.
//

use tracing::{info, warn};

use crate::client::tdkserver::render::report::helper::common::{
    kubeconfig_export_command, render_session_lines,
};
use crate::client::tdkserver::render::view::InstanceView;
use crate::runtime::provider::ClusterProvider;

pub fn created_server(view: &InstanceView) {
    if let Some(path) = &view.instance_path {
        info!("Created tabsdata instance at {}.", path.display());
    } else if let Some(namespace) = &view.namespace {
        info!("Created tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Created tabsdata instance.");
    }
    next_steps_after_create(view);
}

fn next_steps_after_create(view: &InstanceView) {
    let kminus = matches!(view.provider, ClusterProvider::Kminus);
    if kminus {
        warn!("The cluster is now running. Next steps before starting the instance:");
    } else {
        warn!("Next steps before starting the instance:");
    }
    warn!(
        "  - Inspect the deployed configs with 'tdkserver config get --folder <folder>' \
         and the placeholders for required secrets with 'tdkserver secret get --folder <folder>'. \
         Use the downloaded files as a base to customize them with your own values."
    );
    warn!(
        "  - Push edits back with 'tdkserver config put --folder <folder>' for configs and \
         'tdkserver secret put --folder <folder>' for secrets."
    );
    if kminus {
        warn!(
            "  - Once the configs and secrets are in place, launch the workload with \
             'tdkserver instance start' or 'tdkserver instance restart' to roll a running \
             instance. The cluster is already running, so 'tdkserver server start' is unnecessary."
        );
    } else {
        warn!(
            "  - The 'get' and 'put' commands talk to the kubernetes cluster directly; \
             cluster lifecycle is out of scope of tdkserver for this provider."
        );
        warn!(
            "  - Once the configs and secrets are in place, launch the workload with \
             'tdkserver instance start' or roll a running instance with \
             'tdkserver instance restart'."
        );
    }
    warn!(
        "  - Reduce typing on subsequent commands by setting this instance with \
         'tdkserver session set ...' — set values become defaults for cluster / instance / \
         namespace flags."
    );
    warn!(
        "  - Drop the 'tdkserver' prefix entirely by entering a tdkserver shell with \
         'tdkserver shell ...' — every command typed inside is implicitly prefixed."
    );
}

pub fn started_server(view: &InstanceView) {
    if let Some(name) = &view.instance_name {
        if let Some(path) = &view.instance_path {
            info!("Started tabsdata server '{name}' at {}.", path.display());
        } else {
            info!("Started tabsdata server '{name}'.");
        }
    } else if let Some(namespace) = &view.namespace {
        info!("Started tabsdata server in namespace '{namespace}'.");
    } else {
        info!("Started tabsdata server.");
    }
    render_session_lines(view);
    if let Some(kubeconfig) = &view.kubeconfig_path {
        info!("To use kubectl against this cluster, run:");
        info!("  {}", kubeconfig_export_command(kubeconfig));
    }
}

pub fn stopped_server(view: &InstanceView) {
    if let Some(name) = &view.instance_name {
        if let Some(path) = &view.instance_path {
            info!("Stopped tabsdata server '{name}' at {}.", path.display());
        } else {
            info!("Stopped tabsdata server '{name}'.");
        }
    } else if let Some(namespace) = &view.namespace {
        info!("Stopped tabsdata server in namespace '{namespace}'.");
    } else {
        info!("Stopped tabsdata server.");
    }
    render_session_lines(view);
}

pub fn restarted_server(view: &InstanceView) {
    if let Some(name) = &view.instance_name {
        if let Some(path) = &view.instance_path {
            info!("Restarted tabsdata server '{name}' at {}.", path.display());
        } else {
            info!("Restarted tabsdata server '{name}'.");
        }
    } else if let Some(namespace) = &view.namespace {
        info!("Restarted tabsdata server in namespace '{namespace}'.");
    } else {
        info!("Restarted tabsdata server.");
    }
    render_session_lines(view);
}
