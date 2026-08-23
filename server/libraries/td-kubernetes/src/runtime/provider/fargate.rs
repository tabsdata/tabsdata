//
// Copyright 2026 Tabsdata Inc.
//

pub mod cluster;
pub mod instance;

use std::path::PathBuf;

use async_trait::async_trait;
use indicatif::MultiProgress;
use kube::Config;
use td_common::status::ExitStatus::NoAction;

use td_common::settings::DEFAULT_INSTANCE;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_MANIFEST, CONTAINER_PYENV_AG_ROOT, CONTAINER_PYENV_FN_ROOT,
    CONTAINER_PYENV_SCRIPTS_FOLDER, CONTAINER_PYENV_TD_ROOT, CONTAINER_RUNTIMES_ROOT,
    DATABASE_SSL_MODE_VERIFY_CA, DATABASE_SSL_ROOT_CERT, FOLDER_REPOSITORY, FOLDER_STORAGE,
    IndexReach, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
};
use td_python::tabsdata::tabsdata_version;

use crate::base::constants::DbManagerMode;
use crate::base::error::provider::ProviderError;
use crate::base::reporter::Reporter;
use crate::client::registry::{
    ClusterViews, InstanceSettings, init_stage_folder_in, instances_root,
};
use crate::client::session::settings::{FargateInputs, Session};
use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::validate_instance_name;
use crate::core::system::exit::{CommandResult, abort_with};
use crate::kube::api::configmap::require_configmap;
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
use crate::runtime::instance::upgrade::guard::ensure_upgraded;
use crate::runtime::provider::fargate::cluster::FargateCluster;
use crate::runtime::provider::fargate::instance::FargateInstance;
use crate::runtime::provider::outcome::{ClusterStopOutcome, ImagesOutcome, InstanceStopOutcome};
use crate::runtime::provider::summary::Summary;
use crate::runtime::provider::{
    CloudProvider, Cluster, ClusterProvider, ContainerProvider, Instance, Provider,
    ServerlessProvider, Transaction, handle_phase_secrets,
};
use crate::runtime::secrets::{SecretGenerator, SecretPhase};

pub struct FargateProvider {
    kubefile: PathBuf,
    context: String,
    cluster_name: String,
    config: Config,
    cluster: FargateCluster,
    instance: FargateInstance,
    inputs: FargateInputs,
    name: String,
    folder: PathBuf,
    #[allow(dead_code)]
    verbose: bool,
}

impl FargateProvider {
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

    async fn apply_workload(&self, multi: &MultiProgress) -> CommandResult {
        self.instance.start(multi).await
    }
    async fn teardown_workload(&self, multi: &MultiProgress) -> CommandResult<bool> {
        self.instance.stop(multi).await
    }
    async fn provision_database_as_pod(&self, mode: DbManagerMode) -> CommandResult {
        let (context, system, profile) = self.runtimes_context().await?;
        let multi = td_common::logging::multi_progress().unwrap_or_default();
        provision_database(
            &context,
            self.cluster_provider(),
            &system,
            &profile,
            &multi,
            mode,
        )
        .await
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
                provider: ClusterProvider::Fargate.as_str().to_owned(),
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

        let inputs = FargateInputs {};

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

        let cluster = FargateCluster::new(config.clone());
        let fargate_instance =
            FargateInstance::new(config.clone(), folder.clone(), cluster_name.clone());

        Ok(Self {
            kubefile,
            context,
            cluster_name,
            config,
            cluster,
            instance: fargate_instance,
            inputs,
            name,
            folder,
            verbose,
        })
    }
}

#[async_trait]
impl Provider for FargateProvider {
    fn name(&self) -> &'static str {
        ClusterProvider::Fargate.as_str()
    }

    fn display_name(&self) -> String {
        format!(
            "{}:{}/{}",
            ClusterProvider::Fargate.as_str(),
            self.context,
            self.config.default_namespace
        )
    }

    fn to_session(&self) -> Session {
        Session::Fargate {
            inputs: self.inputs.clone(),
        }
    }

    fn cluster_provider(&self) -> ClusterProvider {
        ClusterProvider::Fargate
    }

    async fn kube_config(&self) -> CommandResult<Config> {
        Ok(self.config.clone())
    }

    fn system_values(&self) -> CommandResult<SystemValues> {
        system_values_for(
            &self.name,
            &self.config.default_namespace,
            &self.cluster_name,
        )
    }

    fn secret_generators(&self, _phase: SecretPhase) -> Vec<Box<dyn SecretGenerator>> {
        Vec::new()
    }

    fn summary(&self) -> Summary {
        Summary {
            provider: ClusterProvider::Fargate,
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

    async fn ensure_settable_server(&self) -> CommandResult {
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
        self.provision_database_as_pod(DbManagerMode::Create).await
    }

    async fn upgrade_database(&self, _reporter: &dyn Reporter) -> CommandResult {
        self.provision_database_as_pod(DbManagerMode::Upgrade).await
    }

    async fn create_runtimes(&self, _reporter: &dyn Reporter) -> CommandResult {
        self.create_runtimes_as_pod().await
    }

    async fn create_images(&self, _reporter: &dyn Reporter) -> CommandResult<ImagesOutcome> {
        Ok(ImagesOutcome::NotSupported)
    }

    async fn cluster_alive(&self) -> bool {
        self.cluster.is_alive().await
    }

    async fn instance_alive(&self) -> bool {
        self.cluster_alive().await && self.instance.is_alive().await
    }

    fn manages_cluster_lifecycle(&self) -> bool {
        false
    }

    async fn start_cluster_hook(&self, _reporter: &dyn Reporter) -> CommandResult {
        handle_phase_secrets(self, SecretPhase::ClusterStart).await
    }

    async fn stop_cluster(&self, _reporter: &dyn Reporter) -> CommandResult<ClusterStopOutcome> {
        Ok(ClusterStopOutcome::NotSupported)
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
        let context = self.kube_context().await?;
        let instance = self.summary().instance_name.unwrap_or_default();
        ensure_upgraded(&context, &instance).await?;
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

impl ContainerProvider for FargateProvider {}

impl ServerlessProvider for FargateProvider {}

impl CloudProvider for FargateProvider {}

pub fn system_values_for(
    name: &str,
    namespace: &str,
    cluster: &str,
) -> CommandResult<SystemValues> {
    Ok(SystemValues {
        instance_name: Some(name.to_owned()),
        namespace_name: Some(namespace.to_owned()),
        cluster_name: Some(cluster.to_owned()),
        tabsdata_version: Some(tabsdata_version()?),
        runtimes_root: Some(CONTAINER_RUNTIMES_ROOT.to_owned()),
        pyenv_td_root: Some(CONTAINER_PYENV_TD_ROOT.to_owned()),
        pyenv_ag_root: Some(CONTAINER_PYENV_AG_ROOT.to_owned()),
        pyenv_fn_root: Some(CONTAINER_PYENV_FN_ROOT.to_owned()),
        pyenv_scripts_folder: Some(CONTAINER_PYENV_SCRIPTS_FOLDER.to_owned()),
        database_ssl_mode: Some(DATABASE_SSL_MODE_VERIFY_CA.to_owned()),
        database_ssl_root_cert: Some(DATABASE_SSL_ROOT_CERT.to_owned()),
        uv_default_index: Some(SystemValues::uv_default_index(IndexReach::Cluster)?),
        uv_index: Some(SystemValues::uv_index(IndexReach::Cluster)?),
        uv_index_strategy: Some(SystemValues::uv_index_strategy()?),
        ..SystemValues::default()
    })
}
