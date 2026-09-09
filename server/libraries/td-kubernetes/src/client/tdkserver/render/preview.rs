//
// Copyright 2026 Tabsdata Inc.
//

use tracing::info;

use crate::client::tdkserver::render::view::InstanceView;

#[derive(Debug, Clone, Default)]
pub struct SettingsMarkers {
    pub provider: bool,
    pub instance_name: bool,
    pub kubeconfig_path: bool,
    pub context: bool,
    pub namespace: bool,
}

pub fn set_server(view: &InstanceView) {
    info!("About to change the tabsdata session settings:");
    render_view_lines(view);
}

pub fn create_server(view: &InstanceView) {
    info!("About to create tabsdata instance:");
    render_view_lines(view);
}

pub fn start_cluster(view: &InstanceView) {
    info!("About to start kubernetes cluster:");
    render_view_lines(view);
}

pub fn stop_cluster(view: &InstanceView) {
    info!("About to stop kubernetes cluster:");
    render_view_lines(view);
}

pub fn restart_cluster(view: &InstanceView) {
    info!("About to restart kubernetes cluster:");
    render_view_lines(view);
}

pub fn start_instance(view: &InstanceView) {
    info!("About to start tabsdata instance:");
    render_view_lines(view);
}

pub fn stop_instance(view: &InstanceView) {
    info!("About to stop tabsdata instance:");
    render_view_lines(view);
}

pub fn restart_instance(view: &InstanceView) {
    info!("About to restart tabsdata instance:");
    render_view_lines(view);
}

pub fn start_server(view: &InstanceView) {
    info!("About to start tabsdata server:");
    render_view_lines(view);
}

pub fn stop_server(view: &InstanceView) {
    info!("About to stop tabsdata server:");
    render_view_lines(view);
}

pub fn restart_server(view: &InstanceView) {
    info!("About to restart tabsdata server:");
    render_view_lines(view);
}

pub fn init_service(
    instance: &str,
    instance_in_settings: bool,
    instance_defaulted: bool,
    provider: &str,
    provider_in_settings: bool,
    // provider_defaulted: bool,
    loglevel: Option<(&str, bool)>,
) {
    info!("About to initialize tabsdata instance:");
    info!(
        "  - instance.....: {instance}{}",
        origin_marker(instance_in_settings, instance_defaulted)
    );
    info!(
        "  - provider.....: {provider}{}",
        // origin_marker(provider_in_settings, provider_defaulted)
        origin_marker(provider_in_settings, false)
    );
    if let Some((loglevel, loglevel_defaulted)) = loglevel {
        info!(
            "  - log level....: {loglevel}{}",
            origin_marker(false, loglevel_defaulted)
        );
    }
}

pub fn init_cluster_service(view: &InstanceView) {
    info!("About to initialize tabsdata instance:");
    render_view_lines(view);
}

fn origin_marker(in_settings: bool, defaulted: bool) -> &'static str {
    if in_settings {
        " (session)"
    } else if defaulted {
        " (default)"
    } else {
        ""
    }
}

pub fn create_service(view: &InstanceView, markers: &SettingsMarkers) {
    info!("About to create tabsdata instance:");
    render_marked_view_lines(view, markers);
}

pub fn start_service(view: &InstanceView, markers: &SettingsMarkers) {
    info!("About to start tabsdata instance:");
    render_marked_view_lines(view, markers);
}

pub fn stop_service(view: &InstanceView, markers: &SettingsMarkers) {
    info!("About to stop tabsdata instance:");
    render_marked_view_lines(view, markers);
}

pub fn restart_service(view: &InstanceView, markers: &SettingsMarkers) {
    info!("About to restart tabsdata instance:");
    render_marked_view_lines(view, markers);
}

pub fn upgrade_service(view: &InstanceView, markers: &SettingsMarkers) {
    info!("About to upgrade tabsdata instance:");
    render_marked_view_lines(view, markers);
}

pub fn delete_service(view: &InstanceView, markers: &SettingsMarkers, trash: bool) {
    info!("About to delete tabsdata instance:");
    render_marked_view_lines(view, markers);
    if trash {
        info!("  - trash........: the instance folder will be trashed once deleted");
    }
}

pub fn trash_service(instance: &str, source: &str, trash: &str) {
    info!("About to trash tabsdata instance:");
    info!("  - instance name..: {instance}");
    info!("  - instance folder: {source}");
    info!("  - trash folder...: {trash}");
}

pub fn apply_config(view: &InstanceView) {
    info!("About to apply tabsdata configs and restart the instance:");
    render_view_lines(view);
}

pub(crate) fn render_view_lines(view: &InstanceView) {
    info!("  - provider.....: {}", view.provider.as_str());
    if let Some(name) = &view.instance_name {
        info!("  - instance name: {name}");
    }
    if let Some(version) = &view.instance_version {
        info!("  - version......: {version}");
    }
    if let Some(edition) = &view.instance_edition {
        info!("  - edition......: {edition}");
    }
    if let Some(path) = &view.instance_path {
        info!("  - instance path: {}", path.display());
    }
    if let Some(kubeconfig) = &view.kubeconfig_path {
        info!("  - kubeconfig...: {}", kubeconfig.display());
    }
    if let Some(context) = &view.context {
        info!("  - context......: {context}");
    }
    if let Some(cluster_url) = &view.cluster_url {
        info!("  - cluster url..: {cluster_url}");
    }
    if let Some(cluster_name) = &view.cluster_name {
        info!("  - cluster name.: {cluster_name}");
    }
    if let Some(namespace) = &view.namespace {
        info!("  - namespace....: {namespace}");
    }
    if let Some(loglevel) = view.loglevel {
        info!("  - log level....: {}", loglevel.as_str());
    }
}

fn settings_suffix(in_settings: bool) -> &'static str {
    if in_settings { " (session)" } else { "" }
}

pub(crate) fn render_marked_view_lines(view: &InstanceView, markers: &SettingsMarkers) {
    info!(
        "  - provider.....: {}{}",
        view.provider.as_str(),
        settings_suffix(markers.provider)
    );
    if let Some(name) = &view.instance_name {
        info!(
            "  - instance name: {name}{}",
            settings_suffix(markers.instance_name)
        );
    }
    if let Some(path) = &view.instance_path {
        info!("  - instance path: {}", path.display());
    }
    if let Some(kubeconfig) = &view.kubeconfig_path {
        info!(
            "  - kubeconfig...: {}{}",
            kubeconfig.display(),
            settings_suffix(markers.kubeconfig_path)
        );
    }
    if let Some(context) = &view.context {
        info!(
            "  - context......: {context}{}",
            settings_suffix(markers.context)
        );
    }
    if let Some(cluster_url) = &view.cluster_url {
        info!("  - cluster url..: {cluster_url}");
    }
    if let Some(cluster_name) = &view.cluster_name {
        info!("  - cluster name.: {cluster_name}");
    }
    if let Some(namespace) = &view.namespace {
        info!(
            "  - namespace....: {namespace}{}",
            settings_suffix(markers.namespace)
        );
    }
    if let Some(loglevel) = view.loglevel {
        info!("  - log level....: {}", loglevel.as_str());
    }
}
