//
// Copyright 2026 Tabs Data Inc.
//

use tracing::info;

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
