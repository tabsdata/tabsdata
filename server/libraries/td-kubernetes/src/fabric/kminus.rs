//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use async_trait::async_trait;
use tracing::info;

use td_common::env::TABSDATA_HOME_DIR;
use td_common::server::{DEFAULT_INSTANCE, INSTANCES_FOLDER};
use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::client::tdkserver::widgets::spinner;
use crate::constants::{CONFIGMAP_TABSDATA, FABRIC_KMINUS, FOLDER_CLUSTER};
use crate::exit::{Outcome, abort};
use crate::fabric::{Commit, Fabric};
use crate::kminus::action::{InstanceFolderGuard, create_cluster, start_cluster, stop_cluster};
use crate::kminus::model::KminusInstance;
use crate::kubernetes::action::{configmap_exists, create_configmap, delete_configmap};
use crate::kubernetes::client::build_kube_client;
use crate::kubernetes::toolkit::build_kubeconfig;
use crate::profile::load_profile;
use crate::topology::{Topology, topology_for};

pub struct KminusFabric {
    name: String,
    folder: PathBuf,
    instance: KminusInstance,
    topology: &'static dyn Topology,
}

impl KminusFabric {
    pub fn build(cluster: ClusterArguments, instance: InstanceArguments) -> Outcome<Self> {
        if cluster.context().is_some()
            || cluster.kubeconfig().is_some()
            || *cluster.current()
            || cluster.namespace().is_some()
        {
            cerror!(
                "Arguments --context, --kubeconfig, --current, and --namespace are not valid for the '{FABRIC_KMINUS}' fabric."
            );
            return abort(GeneralError);
        }
        if instance.name().is_some() && instance.path().is_some() {
            cerror!("Arguments --name and --path are mutually exclusive.");
            return abort(GeneralError);
        }

        let instances_root = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/"))
            .join(TABSDATA_HOME_DIR)
            .join(INSTANCES_FOLDER);
        let (name, folder) = match (instance.name().as_deref(), instance.path().as_deref()) {
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

        let kminus_instance = KminusInstance::new(folder.join(FOLDER_CLUSTER));
        let topology = topology_for(crate::fabric::ClusterFabric::Kminus);

        Ok(Self {
            name,
            folder,
            instance: kminus_instance,
            topology,
        })
    }

    async fn replace_configmap(&self, data: BTreeMap<String, String>) -> Outcome {
        let kminus_config =
            build_kubeconfig(Some(&self.instance.kubeconfig_path()), None, None).await?;
        let client = build_kube_client(&kminus_config)?;
        let namespace = &kminus_config.default_namespace;
        if configmap_exists(&client, namespace, CONFIGMAP_TABSDATA).await? {
            delete_configmap(&client, namespace, CONFIGMAP_TABSDATA).await?;
        }
        create_configmap(&client, namespace, CONFIGMAP_TABSDATA, data).await
    }

    fn read_staged_profile(&self) -> Outcome<Option<BTreeMap<String, String>>> {
        let staged_folder = self.instance.staging_profile_path();
        if !staged_folder.is_dir() {
            return Ok(None);
        }
        let entries = match fs::read_dir(&staged_folder) {
            Ok(entries) => entries,
            Err(error) => {
                cerror!(
                    "Failed to read staged profile folder '{}': {error}",
                    staged_folder.display()
                );
                return abort(GeneralError);
            }
        };
        let mut data = BTreeMap::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    cerror!(
                        "Failed to read entry under '{}': {error}",
                        staged_folder.display()
                    );
                    return abort(GeneralError);
                }
            };
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let contents = match fs::read_to_string(&path) {
                Ok(contents) => contents,
                Err(error) => {
                    cerror!("Failed to read '{}': {error}", path.display());
                    return abort(GeneralError);
                }
            };
            data.insert(name.to_owned(), contents);
        }
        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(data))
        }
    }
}

#[async_trait]
impl Fabric for KminusFabric {
    fn name(&self) -> &'static str {
        FABRIC_KMINUS
    }

    async fn ensure_creatable(&self) -> Outcome {
        if self.folder.exists() {
            cerror!("Folder {} already exists.", self.folder.display());
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_create_summary(&self, profile: &str) {
        info!("Tabsdata instance to be created:");
        if cfg!(td_k8s_development) {
            info!("  - profile......: {profile}");
        }
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
    }

    async fn prepare_workspace(&self) -> Outcome<Box<dyn Commit>> {
        if let Err(error) = fs::create_dir_all(&self.folder) {
            cerror!(
                "Failed to create instance folder {}: {error}",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        let guard = InstanceFolderGuard::new(self.folder.clone());
        create_cluster(&self.instance)?;
        Ok(Box::new(guard))
    }

    async fn stage_configmap(&self, profile: &str) -> Outcome {
        let configmap_data = load_profile(profile, self.topology)?;
        let staging_profile = self.instance.staging_profile_path();
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

    async fn store_configmap(&self, _profile: &str) -> Outcome {
        Ok(())
    }

    fn report_created(&self) {
        info!("Created tabsdata instance at {}.", self.folder.display());
        info!(
            "Edit '{}' to customize ports or log settings of the instance's kminus cluster.",
            self.instance.sysconfig_path().display()
        );
    }

    async fn ensure_startable(&self) -> Outcome {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist. Run 'create' first.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_start_summary(&self) {
        info!("Tabsdata instance to be started:");
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
    }

    async fn start_runtime(&self) -> Outcome {
        let starting_spinner = spinner("Starting kminus cluster");
        let result = start_cluster(&self.instance).await;
        starting_spinner.finish_and_clear();
        result
    }

    async fn ingest_configmap(&self) -> Outcome {
        let staged = self.read_staged_profile()?;
        let Some(data) = staged else {
            return Ok(());
        };
        self.replace_configmap(data).await?;
        if let Err(error) = fs::remove_dir_all(self.instance.staging_path()) {
            cerror!(
                "Failed to remove staging folder '{}': {error}",
                self.instance.staging_path().display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn report_started(&self) {
        info!(
            "Started tabsdata instance '{}' at {}.",
            self.name,
            self.folder.display()
        );
        info!(
            "export KUBECONFIG={}",
            self.instance.kubeconfig_path().display()
        );
    }

    async fn ensure_stoppable(&self) -> Outcome {
        if !self.folder.exists() {
            cerror!(
                "Instance folder '{}' does not exist.",
                self.folder.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    fn preview_stop_summary(&self) {
        info!("Tabsdata instance to be stopped:");
        info!("  - fabric.......: {FABRIC_KMINUS}");
        info!("  - instance name: {}", self.name);
        info!("  - instance path: {}", self.folder.display());
    }

    async fn stop_runtime(&self) -> Outcome {
        let stopping_spinner = spinner("Stopping kminus cluster");
        let result = stop_cluster(&self.instance).await;
        stopping_spinner.finish_and_clear();
        result
    }

    fn report_stopped(&self) {
        info!(
            "Stopped tabsdata instance '{}' at {}.",
            self.name,
            self.folder.display()
        );
    }
}
