//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

use tracing::info;

use crate::client::tdkserver::render::view::InstanceView;

pub(crate) const STATUS_RUNNING: &str = "Running";
pub(crate) const STATUS_STOPPED: &str = "Stopped";
pub(crate) const UNGATHERED_CELL: &str = "?";
pub(crate) const ABSENT_CELL: &str = "-";

pub(crate) fn activation_command(activate: &Path) -> String {
    if cfg!(target_os = "windows") {
        activate.display().to_string()
    } else {
        format!("source {}", activate.display())
    }
}

pub(crate) fn kubeconfig_export_command(kubeconfig: &Path) -> String {
    if cfg!(target_os = "windows") {
        format!("set KUBECONFIG={}", kubeconfig.display())
    } else {
        format!("export KUBECONFIG={}", kubeconfig.display())
    }
}

pub(crate) fn render_session_lines(view: &InstanceView) {
    if let Some(kubeconfig) = &view.kubeconfig_path {
        info!("  - kubeconfig...: {}", kubeconfig.display());
    }
    if let Some(context) = &view.context {
        info!("  - context......: {context}");
    }
    if let Some(cluster_name) = &view.cluster_name {
        info!("  - cluster......: {cluster_name}");
    }
    if let Some(namespace) = &view.namespace {
        info!("  - namespace....: {namespace}");
    }
}
