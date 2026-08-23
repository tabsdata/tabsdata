//
// Copyright 2026 Tabsdata Inc.
//

pub mod status;

use std::path::Path;

use tracing::info;

use crate::base::constants::{DEFAULT_LOG_LEVEL, INIT_FOLDER, INPUT_FOLDER};
#[cfg(feature = "enterprise")]
use crate::client::registry::ag_venv_activate_file;
use crate::client::registry::{InstanceSettings, td_venv_activate_file};
use crate::client::tdkserver::render::preview::render_summary_lines;
use crate::client::tdkserver::render::report::helper::common::{
    ABSENT_CELL, STATUS_RUNNING, STATUS_STOPPED, UNGATHERED_CELL, activation_command,
    kubeconfig_export_command, render_session_lines,
};
use crate::client::tdkserver::render::report::helper::table::{
    ReportColumn, ReportTable, STATUS_COLOR_SCHEME, render_tables,
};
use crate::client::tdkserver::widgets::show_hint;
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::ports::{AssignedPort, apiserver_public_port, database_port};
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::summary::Summary;

fn list_columns() -> [ReportColumn<'static>; 9] {
    [
        ReportColumn {
            header: "Name",
            color_scheme: None,
        },
        ReportColumn {
            header: "Folder",
            color_scheme: None,
        },
        ReportColumn {
            header: "Provider",
            color_scheme: None,
        },
        ReportColumn {
            header: "Version",
            color_scheme: None,
        },
        ReportColumn {
            header: "State",
            color_scheme: None,
        },
        ReportColumn {
            header: "Status",
            color_scheme: Some(STATUS_COLOR_SCHEME),
        },
        ReportColumn {
            header: "API Server Port",
            color_scheme: None,
        },
        ReportColumn {
            header: "Database Port",
            color_scheme: None,
        },
        ReportColumn {
            header: "API Server URL",
            color_scheme: None,
        },
    ]
}

pub struct InstanceRow<'a> {
    pub name: &'a str,
    pub folder: &'a Path,
    pub settings: Option<&'a InstanceSettings>,
    pub alive: Option<bool>,
    pub url: Option<String>,
}

pub fn instance_list(instances: &[InstanceRow]) -> CommandResult {
    let rows = instances.iter().map(instance_row).collect();
    let columns = list_columns();
    render_tables(&[ReportTable {
        title: "Instances",
        columns: &columns,
        rows,
    }])
}

fn instance_row(row: &InstanceRow) -> Vec<String> {
    let provider = row
        .settings
        .map(|settings| settings.provider.as_str().to_owned())
        .unwrap_or_else(|| UNGATHERED_CELL.to_owned());
    let version = row
        .settings
        .map(version_cell)
        .unwrap_or_else(|| UNGATHERED_CELL.to_owned());
    let state = row
        .settings
        .map(|settings| settings.state.as_str().to_owned())
        .unwrap_or_else(|| UNGATHERED_CELL.to_owned());

    let status = match row.alive {
        Some(true) => STATUS_RUNNING.to_owned(),
        Some(false) => STATUS_STOPPED.to_owned(),
        None => UNGATHERED_CELL.to_owned(),
    };

    let public_port = port_cell(row.settings, apiserver_public_port);
    let database = port_cell(row.settings, database_port);

    let url = match row.alive {
        Some(true) => row
            .url
            .clone()
            .unwrap_or_else(|| UNGATHERED_CELL.to_owned()),
        Some(false) | None => UNGATHERED_CELL.to_owned(),
    };

    vec![
        row.name.to_owned(),
        row.folder.display().to_string(),
        provider,
        version,
        state,
        status,
        public_port,
        database,
        url,
    ]
}

fn port_cell(
    settings: Option<&InstanceSettings>,
    select: fn(&[AssignedPort]) -> Option<u16>,
) -> String {
    match settings {
        None => UNGATHERED_CELL.to_owned(),
        Some(settings) => select(&settings.ports)
            .map(|port| port.to_string())
            .unwrap_or_else(|| ABSENT_CELL.to_owned()),
    }
}

fn version_cell(settings: &InstanceSettings) -> String {
    settings.version.clone()
}

pub fn instance_inspect(settings: &InstanceSettings, folder: &Path, alive: Option<bool>) {
    let resolved = &settings.cluster.resolved;
    let summary = Summary {
        provider: settings.provider,
        instance_name: Some(settings.name.clone()),
        instance_path: Some(folder.to_path_buf()),
        kubeconfig_path: resolved.kubeconfig.clone(),
        context: resolved.context.clone(),
        cluster_url: resolved.url.clone(),
        cluster_name: resolved.cluster.clone(),
        namespace: resolved.namespace.clone(),
        loglevel: (settings.provider == ClusterProvider::Kminus)
            .then(|| settings.loglevel.unwrap_or(DEFAULT_LOG_LEVEL)),
    };

    let status = match alive {
        Some(true) => STATUS_RUNNING,
        Some(false) => STATUS_STOPPED,
        None => UNGATHERED_CELL,
    };

    info!("Tabsdata instance '{}':", settings.name);
    info!("  - state........: {}", settings.state.as_str());
    info!("  - status.......: {status}");
    info!("  - locality.....: {}", settings.locality.as_str());
    info!("  - version......: {}", version_cell(settings));
    render_summary_lines(&summary);
    info!("Assigned ports:");
    for entry in &settings.ports {
        info!("  - {}: {}", entry.caption, entry.port);
    }

    if let Ok(td_activate) = td_venv_activate_file(&settings.name) {
        info!("Environment activation:");
        info!("  td: {}", activation_command(&td_activate));
        #[cfg(feature = "enterprise")]
        if let Ok(ag_activate) = ag_venv_activate_file(&settings.name) {
            info!("  ai: {}", activation_command(&ag_activate));
        }
    }

    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("Kubectl access:");
        info!("  {}", kubeconfig_export_command(kubeconfig));
    }
}

pub fn created_service(summary: &Summary, with_hint: bool) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Created tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("Created tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Created tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Created tabsdata instance.");
    }
    render_session_lines(summary);
    if with_hint {
        match &summary.instance_name {
            Some(name) => show_hint(
                &format!("Run 'tdkserver start --instance {name}' to start the instance."),
                80,
                true,
            ),
            None => show_hint("Run 'tdkserver start' to start the instance.", 80, true),
        }
    }
}

pub fn started_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Started tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("Started tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Started tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Started tabsdata instance.");
    }
    render_session_lines(summary);
    if let Some(kubeconfig) = &summary.kubeconfig_path {
        info!("To use kubectl against this cluster, run:");
        info!("  {}", kubeconfig_export_command(kubeconfig));
    }
}

pub fn deleted_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        info!("Deleted tabsdata instance '{name}'. Its configuration is kept for a new create.");
    } else if let Some(namespace) = &summary.namespace {
        info!("Deleted tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Deleted tabsdata instance.");
    }
    render_session_lines(summary);
}

pub fn stopped_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!("Stopped tabsdata instance '{name}' at {}.", path.display());
        } else {
            info!("Stopped tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Stopped tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Stopped tabsdata instance.");
    }
    render_session_lines(summary);
}

pub fn restarted_service(summary: &Summary) {
    if let Some(name) = &summary.instance_name {
        if let Some(path) = &summary.instance_path {
            info!(
                "Restarted tabsdata instance '{name}' at {}.",
                path.display()
            );
        } else {
            info!("Restarted tabsdata instance '{name}'.");
        }
    } else if let Some(namespace) = &summary.namespace {
        info!("Restarted tabsdata instance in namespace '{namespace}'.");
    } else {
        info!("Restarted tabsdata instance.");
    }
    render_session_lines(summary);
}

pub fn initialized_service(settings: &InstanceSettings, with_hint: bool) {
    info!("Initialized tabsdata instance '{}'.", settings.name);
    info!("  - state........: {}", settings.state.as_str());
    info!("  - version......: {}", version_cell(settings));
    if settings.provider == ClusterProvider::Kminus {
        match settings.loglevel {
            Some(loglevel) => info!("  - log level....: {}", loglevel.as_str()),
            None => info!(
                "  - log level....: {} (default)",
                DEFAULT_LOG_LEVEL.as_str()
            ),
        }
    }
    let resolved = &settings.cluster.resolved;
    let summary = Summary {
        provider: settings.provider,
        instance_name: Some(settings.name.clone()),
        instance_path: None,
        kubeconfig_path: resolved.kubeconfig.clone(),
        context: resolved.context.clone(),
        cluster_url: resolved.url.clone(),
        cluster_name: resolved.cluster.clone(),
        namespace: resolved.namespace.clone(),
        loglevel: None,
    };
    render_summary_lines(&summary);
    if with_hint {
        let input = settings.folder.join(INIT_FOLDER).join(INPUT_FOLDER);
        show_hint(
            &format!(
                "You can inspect and edit the config in '{}'. To create the instance, run \
                 'tdkserver create --instance {}'.",
                input.display(),
                settings.name
            ),
            80,
            true,
        );
    }
}
