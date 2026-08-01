//
// Copyright 2026 Tabsdata Inc.
//

pub mod cluster;
pub mod instance;

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use indicatif::MultiProgress;
use kube::Config;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::NoAction;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_MANIFEST, CONTAINER_PYENV_AG_ROOT, CONTAINER_PYENV_TD_ROOT,
    CONTAINER_RUNTIMES_ROOT, CONTAINER_TABSDATA_ROOT, DATABASE_SSL_MODE_PREFER, FOLDER_REPOSITORY,
    FOLDER_STORAGE, IndexReach, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY,
    KPROFILE_PODS_FAMILY, YAML_NULL,
};
use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::client::registry::{
    ClusterViews, InstanceSettings, init_stage_folder_in, instances_root,
};
use crate::client::session::pin::{DockerInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::core::system::exit::{CommandResult, abort_with};
use crate::kube::api::configmap::{get_configmap, require_configmap};
use crate::kube::api::{ensure_cluster_reachable, ensure_namespace_exists, exists_configmap};
use crate::kube::context::KubeContext;
use crate::kube::kubeconfig::{
    build_kubeconfig, resolve_cluster_name, resolve_current_context, resolve_kubefile,
};
use crate::runtime::gateway::resolver::SystemValues;
use crate::runtime::instance::cluster::{ClusterDescriptor, ClusterProperties, ClusterSettings};
use crate::runtime::instance::launcher::{provision_database, provision_pyvenvs};
use crate::runtime::instance::manifest::MANIFEST_FILE;
use crate::runtime::instance::ports::apply_ports;
use crate::runtime::instance::profile::{Profile, parse_from_configmaps, read_family_configmaps};
use crate::runtime::instance::staging::promote_staged_profile;
use crate::runtime::instance::verify_manifest_compatibility;
use crate::runtime::provider::docker::cluster::DockerCluster;
use crate::runtime::provider::docker::instance::DockerInstance;
use crate::runtime::provider::outcome::{ClusterStopOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    Cluster, ClusterProvider, ContainerProvider, HostedProvider, Instance, LocalProvider, Provider,
    Transaction, handle_phase_secrets,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct DockerProvider {
    kubefile: PathBuf,
    context: String,
    cluster_name: String,
    config: Config,
    cluster: DockerCluster,
    instance: DockerInstance,
    inputs: DockerInputs,
    name: String,
    folder: PathBuf,
    verbose: bool,
}

impl DockerProvider {
    async fn promote_staged_configmaps(&self) -> CommandResult {
        let context = self.kube_context().await?;
        let stage = init_stage_folder_in(&self.folder);
        if !promote_staged_profile(&context, &stage, &self.folder).await? {
            return Err(ProviderError::StagedProfileMissing(stage.display().to_string()).into());
        }
        Ok(())
    }

    fn bootstrap_folders(&self) -> CommandResult {
        let repository = self.folder.join(FOLDER_REPOSITORY);
        for subfolder in [&repository, &repository.join(FOLDER_STORAGE)] {
            std::fs::create_dir_all(subfolder).map_err(|error| {
                ProviderError::InstanceFolderCreateFailed(
                    subfolder.display().to_string(),
                    error.to_string(),
                )
            })?;
        }
        Ok(())
    }

    async fn runtimes_context(&self) -> CommandResult<(KubeContext, SystemValues, Profile)> {
        let context = self.kube_context().await?;
        let pods = read_family_configmaps(&context, &KPROFILE_PODS_FAMILY).await?;
        let assets = read_family_configmaps(&context, &KPROFILE_ASSETS_FAMILY).await?;
        let configs = read_family_configmaps(&context, &KPROFILE_CONFIGS_FAMILY).await?;
        let mut manifest_data = require_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?;
        let Some(manifest_yaml) = manifest_data.remove(MANIFEST_FILE) else {
            return Err(ProviderError::ConfigmapEntryMissing(
                CONFIGMAP_TABSDATA_MANIFEST.to_owned(),
                MANIFEST_FILE.to_owned(),
            )
            .into());
        };
        let profile = parse_from_configmaps(&pods, &assets, &configs, &manifest_yaml)?;

        let mut system = self.system_values()?;
        let settings = InstanceSettings::load(&self.name)?;
        apply_ports(&mut system, &settings.ports);

        Ok((context, system, profile))
    }

    async fn create_runtimes_as_pod(&self) -> CommandResult {
        let (context, system, profile) = self.runtimes_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        provision_pyvenvs(&context, self.cluster_provider(), &system, &profile, &multi).await
    }

    pub async fn build(
        cluster: ClusterArguments,
        instance: InstanceArguments,
        verbose: bool,
    ) -> CommandResult<Self> {
        if cluster.kubeconfig.is_none() && cluster.context.is_none() && !cluster.current {
            return Err(ProviderError::ArgumentRequired {
                argument: "kubeconfig | context | current".to_owned(),
                provider: ClusterProvider::Docker.as_str().to_owned(),
            }
            .into());
        }
        if cluster.context.is_some() && cluster.current {
            return Err(ProviderError::MutuallyExclusiveArguments {
                first: "context".to_owned(),
                second: "current".to_owned(),
            }
            .into());
        }

        let inputs = DockerInputs {
            kubeconfig: cluster.kubeconfig.clone(),
            context: cluster.context.clone(),
            current: cluster.current,
            namespace: cluster.namespace.clone(),
        };

        let kubefile_argument = cluster.kubeconfig.as_deref();
        let context_argument = cluster.context.as_deref();
        let namespace_override = cluster.namespace.as_deref();

        let Some(kubefile) = resolve_kubefile(kubefile_argument) else {
            return Err(ProviderError::KubeconfigNotFound.into());
        };

        let mut config =
            build_kubeconfig(kubefile_argument, context_argument, namespace_override).await?;

        let Some(context) = cluster
            .context
            .clone()
            .or_else(|| resolve_current_context(kubefile_argument))
        else {
            return Err(ProviderError::KubeconfigContextMissing.into());
        };

        let Some(cluster_name) = resolve_cluster_name(kubefile_argument, Some(&context)) else {
            return Err(ProviderError::ContextClusterMissing(context).into());
        };

        if config.default_namespace.trim().is_empty() {
            config.default_namespace = DEFAULT_INSTANCE.to_string();
        }

        let name = instance
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_INSTANCE.to_string());
        let folder = instances_root()?.join(&name);

        if let Err(error) = validate_instance_name(&name) {
            return Err(ProviderError::InvalidInstanceName {
                name: name.clone(),
                reason: error.to_string(),
            }
            .into());
        }

        let cluster = DockerCluster::new(config.clone());
        let docker_instance = DockerInstance::new(config.clone(), folder.clone());

        Ok(Self {
            kubefile,
            context,
            cluster_name,
            config,
            cluster,
            instance: docker_instance,
            inputs,
            name,
            folder,
            verbose,
        })
    }
}

#[async_trait]
impl Provider for DockerProvider {
    fn name(&self) -> &'static str {
        ClusterProvider::Docker.as_str()
    }

    fn display_name(&self) -> String {
        format!(
            "{}:{}/{}",
            ClusterProvider::Docker.as_str(),
            self.context,
            self.config.default_namespace
        )
    }

    fn to_session(&self) -> Session {
        Session::Docker {
            kubeconfig: self.kubefile.clone(),
            context: self.context.clone(),
            cluster: self.cluster_name.clone(),
            namespace: self.config.default_namespace.clone(),
            inputs: self.inputs.clone(),
        }
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Docker
    }

    async fn kube_config(&self) -> CommandResult<kube::Config> {
        Ok(self.config.clone())
    }

    fn system_values(&self) -> CommandResult<SystemValues> {
        system_values_for(&self.name, &self.config.default_namespace, &self.folder)
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        Vec::new()
    }

    fn summary(&self) -> Summary {
        Summary {
            provider: ClusterProvider::Docker,
            instance_name: Some(self.name.clone()),
            instance_path: None,
            kubeconfig_path: Some(self.kubefile.clone()),
            context: Some(self.context.clone()),
            cluster_url: Some(self.config.cluster_url.to_string()),
            cluster_name: Some(self.cluster_name.clone()),
            namespace: Some(self.config.default_namespace.clone()),
            loglevel: None,
        }
    }

    fn cluster_settings(&self, _provided: ClusterProperties) -> CommandResult<ClusterSettings> {
        Ok(ClusterSettings {
            kubeconfig: Some(self.kubefile.clone()),
            context: Some(self.context.clone()),
            namespace: Some(self.config.default_namespace.clone()),
        })
    }

    fn cluster_descriptor(&self, settings: ClusterSettings) -> CommandResult<ClusterDescriptor> {
        Ok(ClusterDescriptor {
            kubeconfig: settings.kubeconfig,
            context: settings.context,
            namespace: settings.namespace,
            cluster: Some(self.cluster_name.clone()),
            url: Some(self.config.cluster_url.to_string()),
        })
    }

    fn refresh_cluster_views(&self, current: ClusterViews) -> CommandResult<ClusterViews> {
        Ok(current)
    }

    async fn ensure_pinnable_server(&self) -> CommandResult {
        let context = self.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if !exists_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await? {
            return Err(
                ProviderError::InstanceNotFound(self.config.default_namespace.clone()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_creatable_server(&self) -> CommandResult {
        let context = self.kube_context().await?;
        ensure_cluster_reachable(context.client()).await?;
        ensure_namespace_exists(&context).await?;
        if exists_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await? {
            return abort_with(
                NoAction,
                ProviderError::InstanceAlreadyExists(self.config.default_namespace.clone()),
            );
        }
        Ok(())
    }

    async fn create_cluster_hook(
        &self,
        reporter: &dyn Reporter,
    ) -> CommandResult<Box<dyn Transaction>> {
        let step = reporter.step("Preparing kubernetes cluster");
        self.bootstrap_folders()?;
        self.cluster.create()?;
        step.done("Kubernetes cluster prepared");
        let step = reporter.step("Storing tabsdata configmaps");
        self.promote_staged_configmaps().await?;
        step.done("Tabsdata configmaps stored");
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(Box::new(()))
    }

    async fn create_database(&self, _reporter: &dyn Reporter) -> CommandResult {
        let (context, system, profile) = self.runtimes_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        provision_database(&context, self.cluster_provider(), &system, &profile, &multi).await
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> CommandResult {
        self.create_runtimes_as_pod().await
    }

    async fn create_images(&self, _reporter: &dyn Reporter) -> CommandResult {
        Ok(())
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn instance_alive(&self) -> bool {
        self.cluster_alive().await && self.instance.is_alive().await
    }

    async fn start_cluster_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        let step = reporter.step("Starting kubernetes cluster");
        self.cluster.start().await?;
        step.done("Kubernetes cluster started");
        handle_phase_secrets(self, SecretPhase::ClusterStart).await?;
        Ok(())
    }

    async fn stop_cluster(&self, reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome> {
        let step = reporter.step("Stopping kubernetes cluster");
        let stopped = self.cluster.stop().await?;
        step.done("Kubernetes cluster stopped");
        match stopped {
            true => Ok(ClusterStopOutcome::Stopped),
            false => Ok(ClusterStopOutcome::AlreadyStopped),
        }
    }

    async fn ensure_startable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_stoppable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn ensure_restartable_cluster(&self) -> CommandResult {
        Ok(())
    }

    async fn start_instance_hook(&self, reporter: &dyn Reporter) -> CommandResult {
        handle_phase_secrets(self, SecretPhase::InstanceStart).await?;
        let step = reporter.step("Starting tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        self.apply_workload(&multi).await?;
        step.done("Tabsdata instance started");
        Ok(())
    }

    async fn stop_instance(&self, reporter: &dyn Reporter) -> CommandResult<InstanceStopOutcome> {
        let step = reporter.step("Stopping tabsdata instance");
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        let stopped = self.teardown_workload(&multi).await?;
        step.done("Tabsdata instance stopped");
        match stopped {
            true => Ok(InstanceStopOutcome::Stopped),
            false => Ok(InstanceStopOutcome::AlreadyStopped),
        }
    }

    async fn ensure_startable_instance(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_stoppable_instance(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_restartable_instance(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_startable_server(&self) -> CommandResult {
        if cfg!(td_k8s_development) {
            let context = self.kube_context().await?;
            if let Some(data) = get_configmap(&context, CONFIGMAP_TABSDATA_MANIFEST).await?
                && let Some(yaml) = data.get(MANIFEST_FILE)
            {
                verify_manifest_compatibility(yaml)?;
            }
        }
        Ok(())
    }

    async fn ensure_stoppable_server(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }

    async fn ensure_restartable_server(&self) -> CommandResult {
        if !self.folder.exists() {
            return Err(
                ProviderError::InstanceFolderNotFound(self.folder.display().to_string()).into(),
            );
        }
        Ok(())
    }
}

impl ContainerProvider for DockerProvider {}

impl HostedProvider for DockerProvider {}

impl LocalProvider for DockerProvider {}

impl DockerProvider {
    async fn apply_workload(&self, multi: &MultiProgress) -> CommandResult {
        self.instance.start(multi).await
    }

    async fn teardown_workload(&self, multi: &MultiProgress) -> CommandResult<bool> {
        self.instance.stop(multi).await
    }
}

fn container_storage_url() -> String {
    format!("file://{CONTAINER_TABSDATA_ROOT}/{FOLDER_REPOSITORY}/{FOLDER_STORAGE}/")
}

pub fn system_values_for(
    name: &str,
    namespace: &str,
    folder: &Path,
) -> CommandResult<SystemValues> {
    let repository_folder = folder.join(FOLDER_REPOSITORY);
    let storage_folder = repository_folder.join(FOLDER_STORAGE);
    Ok(SystemValues {
        instance_name: Some(name.to_owned()),
        namespace_name: Some(namespace.to_owned()),
        runtimes_root: Some(CONTAINER_RUNTIMES_ROOT.to_owned()),
        pyenv_td_root: Some(CONTAINER_PYENV_TD_ROOT.to_owned()),
        pyenv_ag_root: Some(CONTAINER_PYENV_AG_ROOT.to_owned()),
        database_ssl_mode: Some(DATABASE_SSL_MODE_PREFER.to_owned()),
        database_ssl_root_cert: Some(YAML_NULL.to_owned()),
        repository_path: Some(repository_folder.to_string_lossy().to_string()),
        storage_path: Some(storage_folder.to_string_lossy().to_string()),
        storage_url: Some(container_storage_url()),
        uv_default_index: Some(SystemValues::uv_default_index()?),
        uv_extra_index_url: Some(SystemValues::uv_extra_index_url(IndexReach::HostContainer)?),
        uv_index_strategy: Some(SystemValues::uv_index_strategy()?),
        ..SystemValues::default()
    })
}
