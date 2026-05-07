//
// Copyright 2026 Tabs Data Inc.
//

pub mod cluster;
pub mod instance;

use std::fs;
use std::path::PathBuf;

use td_common::env::TABSDATA_HOME_DIR;
use td_common::server::{DEFAULT_INSTANCE, INSTANCES_FOLDER};
use td_common::status::ExitStatus::GeneralError;
use tracing::info;

use crate::cerror;
use crate::client::tdkcontext::{KminusInputs, TdkContext};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::client::tdkserver::widgets::spinner;
use crate::constants::{FABRIC_KMINUS, FOLDER_CLUSTER};
use crate::fabric::kminus::cluster::{InstanceFolderGuard, KminusCluster};
use crate::fabric::kminus::instance::KminusInstance;
use crate::fabric::{Cluster, Fabric, Instance, Transaction};
use crate::instance::profile::load_profile;
use crate::kubernetes::kubeconfig::{
    resolve_cluster_name, resolve_cluster_url, resolve_current_context, resolve_namespace,
};
use crate::system::exit::{Result, abort};
use crate::topology::{Topology, topology_for};

pub struct KminusFabric {
    name: String,
    folder: PathBuf,
    cluster: KminusCluster,
    instance: KminusInstance,
    topology: &'static dyn Topology,
    inputs: KminusInputs,
}

impl KminusFabric {
    pub fn build(cluster: ClusterArguments, instance: InstanceArguments) -> Result<Self> {
        if cluster.context.is_some()
            || cluster.kubeconfig.is_some()
            || cluster.current
            || cluster.namespace.is_some()
        {
            cerror!(
                "Arguments --context, --kubeconfig, --current, and --namespace are not valid for the '{FABRIC_KMINUS}' fabric."
            );
            return abort(GeneralError);
        }
        if instance.name.is_some() && instance.path.is_some() {
            cerror!("Arguments --name and --path are mutually exclusive.");
            return abort(GeneralError);
        }

        let inputs = KminusInputs {
            name: instance.name.clone(),
            path: instance.path.clone(),
        };

        let instances_root = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/"))
            .join(TABSDATA_HOME_DIR)
            .join(INSTANCES_FOLDER);
        let (name, folder) = match (instance.name.as_deref(), instance.path.as_deref()) {
            (None, None) => {
                let name = DEFAULT_INSTANCE.to_string();
                let folder = instances_root.join(&name);
                (name, folder)
            }
            (Some(name), None) => {
                let folder = instances_root.join(name);
                (name.to_owned(), folder)
            }
            (None, Some(path)) => {
                let folder = if path.is_relative() {
                    instances_root.join(path)
                } else {
                    path.to_path_buf()
                };
                let name = folder
                    .file_name()
                    .and_then(|component| component.to_str())
                    .map(str::to_owned)
                    .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
                (name, folder)
            }
            (Some(_), Some(_)) => {
                cerror!("Arguments --name and --path are mutually exclusive.");
                return abort(GeneralError);
            }
        };

        if let Err(error) = validate_instance_name(&name) {
            cerror!("{error}");
            return abort(GeneralError);
        }

        let kminus_cluster = KminusCluster::new(folder.join(FOLDER_CLUSTER));
        let kminus_instance =
            KminusInstance::new(kminus_cluster.kubefile(), kminus_cluster.staging_path());
        let topology = topology_for(crate::fabric::ClusterFabric::Kminus);

        Ok(Self {
            name,
            folder,
            cluster: kminus_cluster,
            instance: kminus_instance,
            topology,
            inputs,
        })
    }

    fn kube_summary(&self) -> KubeSummary {
        let kubefile = self.cluster.kubefile();
        let context = resolve_current_context(Some(&kubefile)).unwrap_or_default();
        let cluster = resolve_cluster_name(Some(&kubefile), Some(&context)).unwrap_or_default();
        let cluster_url = resolve_cluster_url(Some(&kubefile), Some(&context)).unwrap_or_default();
        let namespace =
            resolve_namespace(Some(&kubefile), Some(&context)).unwrap_or_else(|| "default".into());
        KubeSummary {
            kubefile,
            context,
            cluster,
            cluster_url,
            namespace,
        }
    }
}

struct KubeSummary {
    kubefile: PathBuf,
    context: String,
    cluster: String,
    cluster_url: String,
    namespace: String,
}

impl Fabric for KminusFabric {
    fn name(&self) -> &'static str {
        FABRIC_KMINUS
    }

    fn display_name(&self) -> String {
        format!("{FABRIC_KMINUS}:{}", self.name)
    }

    fn to_tdkcontext(&self) -> TdkContext {
        let summary = self.kube_summary();
        TdkContext::Kminus {
            path: self.folder.clone(),
            kubeconfig: summary.kubefile,
            context: summary.context,
            cluster: summary.cluster,
            namespace: summary.namespace,
            inputs: self.inputs.clone(),
        }
    }

    async fn ensure_creatable_runtime(&self) -> Result {
        if self.folder.exists() {
            cerror!("Folder {} already exists.", self.folder.display());
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_create_runtime_summary(&self, profile: &str) {
        info!("About to create tabsdata instance:");
        if cfg!(td_k8s_development) {
            info!("  - profile......: {profile}");
        }
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
    }

    fn report_created_runtime(&self) {
        info!("Created tabsdata instance at {}.", self.folder.display());
        info!(
            "Edit '{}' to customize ports or log settings of the instance's kminus cluster.",
            self.cluster.sysconfig_path().display()
        );
    }

    async fn ensure_usable_runtime(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist. Run 'tdkserver create ...' first.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        if !self.cluster.sysconfig_path().exists() {
            cerror!(
                "Folder '{}' is not a valid kminus instance (missing '{}').",
                self.folder.display(),
                self.cluster.sysconfig_path().display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_use_runtime_summary(&self) {
        let summary = self.kube_summary();
        info!("About to use tabsdata instance:");
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
        info!("  - kubeconfig...: {}", summary.kubefile.display());
        info!("  - context......: {}", summary.context);
        info!("  - cluster url..: {}", summary.cluster_url);
        info!("  - cluster name.: {}", summary.cluster);
        info!("  - namespace....: {}", summary.namespace);
    }

    fn report_used_runtime(&self) {
        let summary = self.kube_summary();
        info!(
            "Now using tabsdata instance '{}' at {}.",
            self.name,
            self.folder.display()
        );
        info!("  - kubeconfig...: {}", summary.kubefile.display());
        info!("  - context......: {}", summary.context);
        info!("  - cluster......: {}", summary.cluster);
        info!("  - namespace....: {}", summary.namespace);
    }

    async fn ensure_startable_runtime(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist. Run 'create' first.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_start_runtime_summary(&self) {
        let summary = self.kube_summary();
        info!("About to start tabsdata instance:");
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
        info!("  - kubeconfig...: {}", summary.kubefile.display());
        info!("  - context......: {}", summary.context);
        info!("  - cluster url..: {}", summary.cluster_url);
        info!("  - cluster name.: {}", summary.cluster);
        info!("  - namespace....: {}", summary.namespace);
    }

    fn report_started_runtime(&self) {
        let summary = self.kube_summary();
        info!(
            "Started tabsdata instance '{}' at {}.",
            self.name,
            self.folder.display()
        );
        info!("  - kubeconfig...: {}", summary.kubefile.display());
        info!("  - context......: {}", summary.context);
        info!("  - cluster......: {}", summary.cluster);
        info!("  - namespace....: {}", summary.namespace);
        info!("To use kubectl against this cluster, run:");
        info!("  export KUBECONFIG={}", summary.kubefile.display());
    }

    async fn ensure_stoppable_runtime(&self) -> Result {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_stop_runtime_summary(&self) {
        let summary = self.kube_summary();
        info!("About to stop tabsdata instance:");
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
        info!("  - kubeconfig...: {}", summary.kubefile.display());
        info!("  - context......: {}", summary.context);
        info!("  - cluster url..: {}", summary.cluster_url);
        info!("  - cluster name.: {}", summary.cluster);
        info!("  - namespace....: {}", summary.namespace);
    }

    fn report_stopped_runtime(&self) {
        let summary = self.kube_summary();
        info!(
            "Stopped tabsdata instance '{}' at {}.",
            self.name,
            self.folder.display()
        );
        info!("  - kubeconfig...: {}", summary.kubefile.display());
        info!("  - context......: {}", summary.context);
        info!("  - cluster......: {}", summary.cluster);
        info!("  - namespace....: {}", summary.namespace);
    }

    async fn prepare_cluster(&self) -> Result<Box<dyn Transaction>> {
        if let Err(error) = fs::create_dir_all(&self.folder) {
            cerror!(
                "Failed to create instance folder {}: {error}",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        let guard = InstanceFolderGuard::new(self.folder.clone());
        self.cluster.create()?;
        Ok(Box::new(guard))
    }

    async fn start_cluster(&self) -> Result {
        let bar = spinner("Starting kminus cluster");
        let result = self.cluster.start().await;
        bar.finish_and_clear();
        result
    }

    async fn stop_cluster(&self) -> Result<bool> {
        let bar = spinner("Stopping kminus cluster");
        let result = self.cluster.stop().await;
        bar.finish_and_clear();
        result
    }

    async fn stage_configmap(&self, profile: &str) -> Result {
        let configmap_data = load_profile(profile, self.topology)?;
        let staging_profile = self.cluster.staging_profile_path();
        if let Err(error) = fs::create_dir_all(&staging_profile) {
            cerror!(
                "Failed to create staging folder '{}': {error}",
                staging_profile.display()
            );
            return abort(GeneralError);
        }
        for (name, contents) in &configmap_data {
            let path = staging_profile.join(name);
            if let Err(error) = fs::write(&path, contents) {
                cerror!("Failed to write '{}': {error}", path.display());
                return abort(GeneralError);
            }
        }
        Ok(())
    }

    async fn store_configmap(&self, _profile: &str) -> Result {
        Ok(())
    }

    async fn start_instance(&self) -> Result {
        self.instance.start().await
    }

    async fn stop_instance(&self) -> Result<bool> {
        self.instance.stop().await
    }
}
