//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::runtime::provider::summary::Summary;

pub fn started_cluster(summary: &Summary) {
    cluster_line("Started", summary);
}

pub fn stopped_cluster(summary: &Summary) {
    cluster_line("Stopped", summary);
}

pub fn restarted_cluster(summary: &Summary) {
    cluster_line("Restarted", summary);
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
