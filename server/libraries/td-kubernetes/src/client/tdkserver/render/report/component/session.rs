//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::client::tdkserver::render::report::helper::common::render_session_lines;
use crate::runtime::provider::summary::Summary;

pub fn set_server(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!(
                "Tabsdata session settings of '{name}' saved at {}.",
                path.display()
            );
        } else {
            info!("Tabsdata session settings of '{name}' saved.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Tabsdata session settings of namespace '{namespace}' saved.");
    } else {
        info!("Tabsdata session settings saved.");
    }
    render_session_lines(summary);
}
