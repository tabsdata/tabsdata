//
// Copyright 2026 Tabsdata Inc.
//

pub mod create;
pub mod delete;
pub mod info;
pub mod init;
pub mod inspect;
pub mod list;
pub mod quickstart;
pub mod register;
pub mod restart;
pub mod start;
pub mod status;
pub mod stop;
pub mod trash;
pub mod unregister;

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;

use crate::client::session::settings::{Resolved, Settings, resolve, resolve_flag};
use crate::client::tdkserver::arguments::ClusterArguments;
use crate::client::tdkserver::render::preview::SettingsMarkers;
use crate::runtime::provider::summary::Summary;

#[derive(Debug, Clone, Default)]
pub struct ClusterSet {
    pub provider: bool,
    pub kubeconfig: bool,
    pub context: bool,
    pub namespace: bool,
}

#[derive(Debug, Clone, Args)]
pub struct InstanceNameArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Registered instance name. \
                     When omitted, the set default instance is used; \
                     when no instance is set, the 'tabsdata' instance is used."
    )]
    pub instance: Option<String>,
}

pub fn resolved_instance(user: Option<String>, settings: &Settings) -> Resolved<Option<String>> {
    resolve(user, settings.instance.clone())
}

pub fn resolved_details(user: Option<bool>, settings: &Settings) -> Resolved<bool> {
    resolve_flag(user, settings.details, true)
}

pub fn resolved_yes(user: Option<bool>, settings: &Settings) -> Resolved<bool> {
    resolve_flag(user, settings.autoconfirm, false)
}

pub fn resolved_save(user: Option<bool>, settings: &Settings) -> Resolved<bool> {
    resolve_flag(user, settings.save, false)
}

pub fn instance_markers(summary: &Summary, instance_in_settings: bool) -> SettingsMarkers {
    SettingsMarkers {
        instance_name: instance_in_settings && summary.instance_name.is_some(),
        ..SettingsMarkers::default()
    }
}

pub fn instance_label(summary: &Summary) -> &str {
    summary.instance_name.as_deref().unwrap_or(DEFAULT_INSTANCE)
}

pub fn resolved_cluster(
    cluster: ClusterArguments,
    settings: &Settings,
) -> (ClusterArguments, ClusterSet) {
    let provider = resolve(cluster.provider, settings.provider);
    let kubeconfig = resolve(cluster.kubeconfig, settings.kubeconfig.clone());
    let context = resolve(cluster.context, settings.context.clone());
    let namespace = resolve(cluster.namespace, settings.namespace.clone());
    let resolved = ClusterArguments {
        provider: provider.value,
        context: context.value,
        kubeconfig: kubeconfig.value,
        current: cluster.current,
        namespace: namespace.value,
    };
    let set = ClusterSet {
        provider: provider.in_settings,
        kubeconfig: kubeconfig.in_settings,
        context: context.in_settings,
        namespace: namespace.in_settings,
    };
    (resolved, set)
}
