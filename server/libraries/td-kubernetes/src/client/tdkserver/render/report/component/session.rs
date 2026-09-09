//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::client::tdkserver::render::report::helper::common::render_session_lines;
use crate::client::tdkserver::render::view::InstanceView;

pub fn set_server(view: &InstanceView) {
    if let Some(name) = &view.instance_name {
        if let Some(path) = &view.instance_path {
            info!(
                "Tabsdata session settings of '{name}' saved at {}.",
                path.display()
            );
        } else {
            info!("Tabsdata session settings of '{name}' saved.");
        }
    } else if let Some(namespace) = &view.namespace {
        info!("Tabsdata session settings of namespace '{namespace}' saved.");
    } else {
        info!("Tabsdata session settings saved.");
    }
    render_session_lines(view);
}
