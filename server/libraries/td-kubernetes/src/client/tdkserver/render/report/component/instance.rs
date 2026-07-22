//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::runtime::provider::summary::Summary;

pub fn started_instance(summary: &Summary) {
    instance_line("Started", summary);
}

pub fn stopped_instance(summary: &Summary) {
    instance_line("Stopped", summary);
}

pub fn restarted_instance(summary: &Summary) {
    instance_line("Restarted", summary);
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
