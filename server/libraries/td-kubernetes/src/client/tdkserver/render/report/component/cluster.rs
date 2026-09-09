//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::client::tdkserver::render::view::InstanceView;

pub fn started_cluster(view: &InstanceView) {
    cluster_line("Started", view);
}

pub fn stopped_cluster(view: &InstanceView) {
    cluster_line("Stopped", view);
}

pub fn restarted_cluster(view: &InstanceView) {
    cluster_line("Restarted", view);
}

fn cluster_line(verb: &str, view: &InstanceView) {
    if let Some(name) = &view.instance_name {
        if let Some(path) = &view.instance_path {
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
