//
// Copyright 2026 Tabs Data Inc.
//

use tracing::info;

use crate::runtime::provider::summary::Summary;

pub(super) fn render_session_lines(summary: &Summary) {
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
