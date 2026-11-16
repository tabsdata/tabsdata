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
    export_command(kubeconfig, cfg!(target_os = "windows"))
}

fn export_command(kubeconfig: &Path, powershell: bool) -> String {
    let path = kubeconfig.display().to_string();
    if powershell {
        format!("$env:KUBECONFIG = '{}'", path.replace('\'', "''"))
    } else {
        format!("export KUBECONFIG='{}'", path.replace('\'', r"'\''"))
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::export_command;

    #[test]
    fn powershell_assigns_the_env_drive_with_single_quotes() {
        let path = Path::new("C:/Users/O'Neil/.tabsdata/kubeconfig.yaml");
        assert_eq!(
            export_command(path, true),
            "$env:KUBECONFIG = 'C:/Users/O''Neil/.tabsdata/kubeconfig.yaml'"
        );
    }

    #[test]
    fn posix_shells_export_a_single_quoted_path() {
        let path = Path::new("/home/o'neil/my instance/kubeconfig.yaml");
        assert_eq!(
            export_command(path, false),
            r"export KUBECONFIG='/home/o'\''neil/my instance/kubeconfig.yaml'"
        );
    }
}
