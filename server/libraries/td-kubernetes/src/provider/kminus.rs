//
// Copyright 2026 Tabs Data Inc.
//

pub mod cluster;
pub mod instance;

use std::fs;
use std::io::Read;
use std::path::PathBuf;

use async_trait::async_trait;
use td_common::env::TABSDATA_HOME_DIR;
use td_common::server::{DEFAULT_INSTANCE, INSTANCES_FOLDER};
use td_common::status::ExitStatus::{GeneralError, NoAction};
use tracing::info;

use crate::cerror;
use crate::client::tdkcontext::{KminusInputs, TdkContext};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::client::tdkserver::widgets::spinner;
use crate::constants::{FOLDER_CLUSTER, PROVIDER_KMINUS};
use crate::instance::manifest::MANIFEST_FILE;
use crate::instance::profile::load_profile;
use crate::kubernetes::kubeconfig::{
    resolve_cluster_name, resolve_cluster_url, resolve_current_context, resolve_namespace,
};
use crate::provider::kminus::cluster::{InstanceFolderGuard, KminusCluster};
use crate::provider::kminus::instance::KminusInstance;
use crate::provider::{Cluster, Instance, Provider, Transaction};
use crate::system::exit::{Result, abort};
use crate::topology::{Topology, topology_for};

pub struct KminusProvider {
    name: String,
    folder: PathBuf,
    cluster: KminusCluster,
    instance: KminusInstance,
    topology: &'static dyn Topology,
    inputs: KminusInputs,
}

impl KminusProvider {
    pub fn build(cluster: ClusterArguments, instance: InstanceArguments) -> Result<Self> {
        if cluster.context.is_some()
            || cluster.kubeconfig.is_some()
            || cluster.current
            || cluster.namespace.is_some()
        {
            cerror!(
                "Arguments --context, --kubeconfig, --current, and --namespace are not valid for the '{PROVIDER_KMINUS}' provider."
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
        let topology = topology_for(crate::provider::ClusterProvider::Kminus);

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

#[async_trait]
impl Provider for KminusProvider {
    fn name(&self) -> &'static str {
        PROVIDER_KMINUS
    }

    fn display_name(&self) -> String {
        format!("{PROVIDER_KMINUS}:{}", self.name)
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
            info!("Folder {} already exists.", self.folder.display());
            return abort(NoAction);
        }
        Ok(())
    }

    fn preview_create_runtime_summary(&self) {
        info!("About to create tabsdata instance:");
        info!("  - provider.....: {PROVIDER_KMINUS}");
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
        info!("  - provider.....: {PROVIDER_KMINUS}");
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
        info!("  - provider.....: {PROVIDER_KMINUS}");
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
        info!("  - provider.....: {PROVIDER_KMINUS}");
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

    async fn stage_configmap(&self) -> Result {
        let loaded = load_profile(self.topology)?;
        let staging_profile = self.cluster.staging_profile_path();
        if let Err(error) = fs::create_dir_all(&staging_profile) {
            cerror!(
                "Failed to create staging folder '{}': {error}",
                staging_profile.display()
            );
            return abort(GeneralError);
        }
        for decoded_path in loaded.manifest.entries.keys() {
            let virtual_target = match loaded.tree.join(decoded_path) {
                Ok(target) => target,
                Err(error) => {
                    cerror!("Failed to resolve virtual path '{decoded_path}': {error}");
                    return abort(GeneralError);
                }
            };
            let mut reader = match virtual_target.open_file() {
                Ok(reader) => reader,
                Err(error) => {
                    cerror!(
                        "Failed to open virtual file '{}': {error}",
                        virtual_target.as_str()
                    );
                    return abort(GeneralError);
                }
            };
            let mut content = Vec::new();
            if let Err(error) = reader.read_to_end(&mut content) {
                cerror!(
                    "Failed to read virtual file '{}': {error}",
                    virtual_target.as_str()
                );
                return abort(GeneralError);
            }
            let disk_path = staging_profile.join(decoded_path);
            if let Some(parent) = disk_path.parent() {
                if let Err(error) = fs::create_dir_all(parent) {
                    cerror!(
                        "Failed to create staging directory '{}': {error}",
                        parent.display()
                    );
                    return abort(GeneralError);
                }
            }
            if let Err(error) = fs::write(&disk_path, &content) {
                cerror!("Failed to write '{}': {error}", disk_path.display());
                return abort(GeneralError);
            }
        }
        let manifest_yaml = match serde_yaml::to_string(&loaded.manifest) {
            Ok(yaml) => yaml,
            Err(error) => {
                cerror!("Failed to serialize the profile manifest: {error}");
                return abort(GeneralError);
            }
        };
        let manifest_path = staging_profile.join(MANIFEST_FILE);
        if let Err(error) = fs::write(&manifest_path, manifest_yaml) {
            cerror!(
                "Failed to write manifest '{}': {error}",
                manifest_path.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn store_configmap(&self) -> Result {
        Ok(())
    }

    async fn start_instance(&self) -> Result {
        self.instance.start().await
    }

    async fn stop_instance(&self) -> Result<bool> {
        self.instance.stop().await
    }
}
