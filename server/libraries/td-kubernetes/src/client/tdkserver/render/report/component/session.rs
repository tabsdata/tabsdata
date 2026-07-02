//
// Copyright 2026 Tabs Data Inc.
//

use tracing::info;

use crate::client::tdkserver::render::report::helper::common::render_session_lines;
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
