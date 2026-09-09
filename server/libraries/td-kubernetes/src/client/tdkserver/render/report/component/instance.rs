//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::client::tdkserver::render::view::InstanceView;

pub fn started_instance(view: &InstanceView) {
    instance_line("Started", view);
}

pub fn stopped_instance(view: &InstanceView) {
    instance_line("Stopped", view);
}

pub fn restarted_instance(view: &InstanceView) {
    instance_line("Restarted", view);
}

fn instance_line(verb: &str, view: &InstanceView) {
    if let Some(name) = &view.instance_name {
        if let Some(path) = &view.instance_path {
            info!("{verb} tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("{verb} tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &view.namespace {
        info!("{verb} tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("{verb} tabsdata instance.");
    }
}
