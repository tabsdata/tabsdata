//
// Copyright 2026 Tabs Data Inc.
//

use tracing::{info, warn};

use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::summary::Summary;

pub fn pinned_server(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Pinned tabsdata session '{name}' at {}.", path.display());
        } else {
            info!("Pinned tabsdata session '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Pinned tabsdata session in namespace '{namespace}'.");
    } else {
        info!("Pinned tabsdata session.");
    }
    render_session_lines(summary);
}

pub fn created_server(summary: &Summary) {
    if let Some(path) = &summary.instance_path {
        info!("Created tabsdata instance at {}.", path.display());
    } else if let Some(namespace) = &summary.namespace {
        info!("Created tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Created tabsdata instance.");
    }
    next_steps_after_create(summary);
}

fn next_steps_after_create(summary: &Summary) {
    let needs_cluster_start = matches!(summary.provider, ClusterProvider::Kminus);
    warn!("Next steps before starting the instance:");
    warn!(
        "  - Inspect the deployed configs with 'tdkserver config get --folder <folder>' \
         and the placeholders for required secrets with 'tdkserver secret get --folder <folder>'. \
         Use the downloaded files as a base to customize them with your own values."
    );
    warn!(
        "  - Push edits back with 'tdkserver config put --folder <folder>' for configs and \
         'tdkserver secret put --folder <folder>' for secrets."
    );
    if needs_cluster_start {
        warn!(
            "  - The 'get' and 'put' commands require a running cluster. \
             Start it with 'tdkserver cluster start' before configuring configs or secrets."
        );
        warn!(
            "  - Once the configs and secrets are in place, launch the workload with \
             'tdkserver instance start' (cluster already running), 'tdkserver server start' \
             (cluster not yet started), or 'tdkserver instance restart' / \
             'tdkserver server restart' to roll a running instance."
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
}

pub fn started_server(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Started tabsdata server '{name}' at {}.", path.display());
        } else {
            info!("Started tabsdata server '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Started tabsdata server in namespace '{namespace}'.");
    } else {
        info!("Started tabsdata server.");
    }
    render_session_lines(summary);
    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("To use kubectl against this cluster, run:");
        info!("  export KUBECONFIG={}", kubeconfig.display());
    }
}

pub fn stopped_server(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Stopped tabsdata server '{name}' at {}.", path.display());
        } else {
            info!("Stopped tabsdata server '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Stopped tabsdata server in namespace '{namespace}'.");
    } else {
        info!("Stopped tabsdata server.");
    }
    render_session_lines(summary);
}

pub fn restarted_server(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Restarted tabsdata server '{name}' at {}.", path.display());
        } else {
            info!("Restarted tabsdata server '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Restarted tabsdata server in namespace '{namespace}'.");
    } else {
        info!("Restarted tabsdata server.");
    }
    render_session_lines(summary);
}

pub fn started_cluster(summary: &Summary) {
    cluster_line("Started", summary);
}

pub fn stopped_cluster(summary: &Summary) {
    cluster_line("Stopped", summary);
}

pub fn restarted_cluster(summary: &Summary) {
    cluster_line("Restarted", summary);
}

pub fn started_instance(summary: &Summary) {
    instance_line("Started", summary);
}

pub fn stopped_instance(summary: &Summary) {
    instance_line("Stopped", summary);
}

pub fn restarted_instance(summary: &Summary) {
    instance_line("Restarted", summary);
}

fn cluster_line(verb: &str, summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!(
                "{verb} kubernetes cluster for instance '{name}' at {}.",
                path.display()
            );
        } else {
            info!("{verb} kubernetes cluster for instance '{name}'.");
        }
    } else {
        info!("{verb} kubernetes cluster.");
    }
}

fn instance_line(verb: &str, summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("{verb} tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("{verb} tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("{verb} tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("{verb} tabsdata instance.");
    }
}

fn render_session_lines(summary: &Summary) {
    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("  - kubeconfig...: {}", kubeconfig.display());
    }
    if let Some(context) = &summary.context {
        info!("  - context......: {context}");
    }
    if let Some(cluster_name) = &summary.cluster_name {
        info!("  - cluster......: {cluster_name}");
    }
    if let Some(namespace) = &summary.namespace {
        info!("  - namespace....: {namespace}");
    }
}
