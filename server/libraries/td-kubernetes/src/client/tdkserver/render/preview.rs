//
// Copyright 2026 Tabs Data Inc.
//

use tracing::info;

use crate::runtime::provider::summary::Summary;

pub fn pin_server(summary: &Summary) {
    info!("About to pin tabsdata session:");
    render_summary_lines(summary);
}

pub fn create_server(summary: &Summary) {
    info!("About to create tabsdata instance:");
    render_summary_lines(summary);
}

pub fn start_cluster(summary: &Summary) {
    info!("About to start kubernetes cluster:");
    render_summary_lines(summary);
}

pub fn stop_cluster(summary: &Summary) {
    info!("About to stop kubernetes cluster:");
    render_summary_lines(summary);
}

pub fn restart_cluster(summary: &Summary) {
    info!("About to restart kubernetes cluster:");
    render_summary_lines(summary);
}

pub fn start_instance(summary: &Summary) {
    info!("About to start tabsdata instance:");
    render_summary_lines(summary);
}

pub fn stop_instance(summary: &Summary) {
    info!("About to stop tabsdata instance:");
    render_summary_lines(summary);
}

pub fn restart_instance(summary: &Summary) {
    info!("About to restart tabsdata instance:");
    render_summary_lines(summary);
}

pub fn start_server(summary: &Summary) {
    info!("About to start tabsdata server:");
    render_summary_lines(summary);
}

pub fn stop_server(summary: &Summary) {
    info!("About to stop tabsdata server:");
    render_summary_lines(summary);
}

pub fn restart_server(summary: &Summary) {
    info!("About to restart tabsdata server:");
    render_summary_lines(summary);
}

pub fn create_service(summary: &Summary) {
    info!("About to create tabsdata instance:");
    render_summary_lines(summary);
}

pub fn start_service(summary: &Summary) {
    info!("About to start tabsdata instance:");
    render_summary_lines(summary);
}

pub fn stop_service(summary: &Summary) {
    info!("About to stop tabsdata instance:");
    render_summary_lines(summary);
}

pub fn restart_service(summary: &Summary) {
    info!("About to restart tabsdata instance:");
    render_summary_lines(summary);
}

pub fn apply_config(summary: &Summary) {
    info!("About to apply tabsdata configs and restart the instance:");
    render_summary_lines(summary);
}

fn render_summary_lines(summary: &Summary) {
    info!("  - provider.....: {}", summary.provider.as_str());
    if let Some(name) = &summary.instance_name {
        info!("  - instance name: {name}");
    }
    if let Some(path) = &summary.instance_path {
        info!("  - instance path: {}", path.display());
    }
    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("  - kubeconfig...: {}", kubeconfig.display());
    }
    if let Some(context) = &summary.context {
        info!("  - context......: {context}");
    }
    if let Some(cluster_url) = &summary.cluster_url {
        info!("  - cluster url..: {cluster_url}");
    }
    if let Some(cluster_name) = &summary.cluster_name {
        info!("  - cluster name.: {cluster_name}");
    }
    if let Some(namespace) = &summary.namespace {
        info!("  - namespace....: {namespace}");
    }
}
