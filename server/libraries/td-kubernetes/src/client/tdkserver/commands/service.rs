//
// Copyright 2026 Tabs Data Inc.
//

pub mod create;
pub mod info;
pub mod init;
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

use crate::client::session::pin::{Pins, Resolved, resolve, resolve_flag};
use crate::client::tdkserver::arguments::ClusterArguments;
use crate::client::tdkserver::render::preview::PinnedMarkers;
use crate::runtime::provider::summary::Summary;

#[derive(Debug, Clone, Default)]
pub struct ClusterPinned {
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
                     When omitted, the pinned default instance is used; \
                     when no instance is pinned, the 'tabsdata' instance is used."
    )]
    pub instance: Option<String>,
}

pub fn resolved_instance(user: Option<String>, pins: &Pins) -> Resolved<Option<String>> {
    resolve(user, pins.instance.clone())
}

pub fn resolved_preview(user: Option<bool>, pins: &Pins) -> Resolved<bool> {
    resolve_flag(user, pins.preview, true)
}

pub fn resolved_yes(user: Option<bool>, pins: &Pins) -> Resolved<bool> {
    resolve_flag(user, pins.autoconfirm, false)
}

pub fn resolved_save(user: Option<bool>, pins: &Pins) -> Resolved<bool> {
    resolve_flag(user, pins.save, false)
}

pub fn instance_markers(summary: &Summary, instance_pinned: bool) -> PinnedMarkers {
    PinnedMarkers {
        instance_name: instance_pinned && summary.instance_name.is_some(),
        ..PinnedMarkers::default()
    }
}

pub fn resolved_cluster(
    cluster: ClusterArguments,
    pins: &Pins,
) -> (ClusterArguments, ClusterPinned) {
    let provider = resolve(cluster.provider, pins.provider);
    let kubeconfig = resolve(cluster.kubeconfig, pins.kubeconfig.clone());
    let context = resolve(cluster.context, pins.context.clone());
    let namespace = resolve(cluster.namespace, pins.namespace.clone());
    let resolved = ClusterArguments {
        provider: provider.value,
        context: context.value,
        kubeconfig: kubeconfig.value,
        current: cluster.current,
        namespace: namespace.value,
    };
    let pinned = ClusterPinned {
        provider: provider.pinned,
        kubeconfig: kubeconfig.pinned,
        context: context.pinned,
        namespace: namespace.pinned,
    };
    (resolved, pinned)
}
