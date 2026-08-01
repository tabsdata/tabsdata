//
// Copyright 2026 Tabsdata Inc.
//

use std::time::Duration;

use clap_derive::ValueEnum;
use const_format::concatcp;
use serde::{Deserialize, Serialize};

use crate::base::error::cli::CliError;

pub const ENV_KUBECONFIG: &str = "KUBECONFIG";
pub const ENV_TD_KUBE_UV_MODE: &str = "TD_KUBE_UV_MODE";

pub const ENV_UV_DEFAULT_INDEX: &str = "UV_DEFAULT_INDEX";
pub const ENV_UV_EXTRA_INDEX_URL: &str = "UV_EXTRA_INDEX_URL";
pub const ENV_UV_INDEX_STRATEGY: &str = "UV_INDEX_STRATEGY";
pub const ENV_UV_PYTHON_INSTALL_DIR: &str = "UV_PYTHON_INSTALL_DIR";

pub const DEVPI_INDEX_URL_HOST: &str = "http://localhost:14159/tabsdata/td/+simple/";
pub const DEVPI_INDEX_URL_CONTAINER: &str =
    "http://host.docker.internal:14159/tabsdata/td/+simple/";
pub const DEVPI_INDEX_URL_CLUSTER: &str =
    "http://devpi.devpi.svc.cluster.local:14159/tabsdata/td/+simple/";

pub const CONTAINER_HOST_GATEWAY: &str = "host.docker.internal";
const LOOPBACK_HOSTS: [&str; 4] = ["localhost", "127.0.0.1", "::1", "[::1]"];

pub const PYPI_INDEX_URL: &str = "https://pypi.org/simple/";

pub const UV_INDEX_STRATEGY_BEST_MATCH: &str = "unsafe-best-match";
pub const UV_INDEX_STRATEGY_FIRST_INDEX: &str = "first-index";

pub const UV_MODE_LOCAL: &str = "local";
pub const UV_MODE_DEVPI: &str = "devpi";
pub const UV_MODE_PYPI: &str = "pypi";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UvMode {
    Local,
    Devpi,
    Pypi,
    Unset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexReach {
    Host,
    HostContainer,
    Cluster,
}

impl UvMode {
    pub fn from_environment() -> Result<Self, CliError> {
        let value = std::env::var(ENV_TD_KUBE_UV_MODE).unwrap_or_default();
        match value.trim().to_lowercase().as_str() {
            UV_MODE_LOCAL => Ok(Self::Local),
            UV_MODE_DEVPI => Ok(Self::Devpi),
            UV_MODE_PYPI => Ok(Self::Pypi),
            "" => Ok(Self::Unset),
            other => Err(CliError::UvModeUnsupported {
                variable: ENV_TD_KUBE_UV_MODE.to_owned(),
                value: other.to_owned(),
                supported: [UV_MODE_LOCAL, UV_MODE_DEVPI, UV_MODE_PYPI].join(", "),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => UV_MODE_LOCAL,
            Self::Devpi => UV_MODE_DEVPI,
            Self::Pypi => UV_MODE_PYPI,
            Self::Unset => "",
        }
    }

    pub fn default_index(self) -> String {
        match self {
            Self::Local => String::new(),
            Self::Devpi | Self::Pypi => PYPI_INDEX_URL.to_owned(),
            Self::Unset => {
                environment_index(ENV_UV_DEFAULT_INDEX).unwrap_or_else(|| PYPI_INDEX_URL.to_owned())
            }
        }
    }

    pub fn extra_index_url(self, reach: IndexReach) -> String {
        match self {
            Self::Local => String::new(),
            Self::Devpi => match reach {
                IndexReach::Host => DEVPI_INDEX_URL_HOST.to_owned(),
                IndexReach::HostContainer => DEVPI_INDEX_URL_CONTAINER.to_owned(),
                IndexReach::Cluster => DEVPI_INDEX_URL_CLUSTER.to_owned(),
            },
            Self::Pypi => String::new(),
            Self::Unset => match environment_index(ENV_UV_EXTRA_INDEX_URL) {
                Some(index) => match reach {
                    IndexReach::Host => index,
                    IndexReach::HostContainer => reach_from_container(&index),
                    IndexReach::Cluster => DEVPI_INDEX_URL_CLUSTER.to_owned(),
                },
                None => String::new(),
            },
        }
    }

    pub fn index_strategy(self) -> String {
        match self {
            Self::Local => UV_INDEX_STRATEGY_FIRST_INDEX.to_owned(),
            Self::Devpi => UV_INDEX_STRATEGY_FIRST_INDEX.to_owned(),
            Self::Pypi => UV_INDEX_STRATEGY_FIRST_INDEX.to_owned(),
            Self::Unset => match environment_index(ENV_UV_INDEX_STRATEGY) {
                Some(strategy) => strategy,
                None if environment_index(ENV_UV_EXTRA_INDEX_URL).is_some() => {
                    UV_INDEX_STRATEGY_BEST_MATCH.to_owned()
                }
                None => UV_INDEX_STRATEGY_FIRST_INDEX.to_owned(),
            },
        }
    }
}

fn reach_from_container(index: &str) -> String {
    let Ok(mut url) = url::Url::parse(index) else {
        return index.to_owned();
    };
    let Some(host) = url.host_str() else {
        return index.to_owned();
    };
    if !LOOPBACK_HOSTS.contains(&host) {
        return index.to_owned();
    }
    if url.set_host(Some(CONTAINER_HOST_GATEWAY)).is_err() {
        return index.to_owned();
    }
    url.to_string()
}

fn environment_index(variable: &str) -> Option<String> {
    std::env::var(variable)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub const FOLDER_CLUSTER: &str = "cluster";
pub const FOLDER_REPOSITORY: &str = "repository";
pub const FOLDER_DATABASE: &str = "database";
pub const FOLDER_STORAGE: &str = "storage";
pub const FOLDER_RUNTIMES: &str = "runtimes";
pub const FOLDER_UV: &str = ".uv";
pub const FOLDER_KENVS: &str = "kenvs";
pub const VENVS_IMAGE: &str = "tdvenvs.tar.zst";
pub const CONTAINER_RUNTIMES_ROOT: &str = "/tabsdata/runtimes";
pub const CONTAINER_PYENV_TD_ROOT: &str = concatcp!(
    CONTAINER_RUNTIMES_ROOT,
    "/",
    FOLDER_UV,
    "/",
    FOLDER_KENVS,
    "/",
    FOLDER_PYENV_TD
);
pub const CONTAINER_PYENV_AG_ROOT: &str = concatcp!(
    CONTAINER_RUNTIMES_ROOT,
    "/",
    FOLDER_UV,
    "/",
    FOLDER_KENVS,
    "/",
    FOLDER_PYENV_AG
);
pub const FOLDER_IMAGES: &str = "images";
pub const FOLDER_PYTHON: &str = "python";
pub const FOLDER_PYENV_TD: &str = "td";
pub const FOLDER_PYENV_AG: &str = "ag";
pub const CONTAINER_PYENV_SCRIPTS_FOLDER: &str = "bin";
pub const FOLDER_STAGING: &str = "staging";
pub const FOLDER_PROFILE: &str = "profile";
pub const FOLDER_KUBE: &str = ".kube";
pub const FILE_KUBECONFIG: &str = "config";

pub const LIBEXEC_TABSDATA: &str = "libexec/tabsdata";

pub const RESOURCE_KPROFILE: &str = "resources/kprofile";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    System,
    Application,
    Function,
    Connection,
}

impl Scope {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Scope::System => "sys",
            Scope::Application => "app",
            Scope::Function => "fn",
            Scope::Connection => "conn",
        }
    }
}

pub struct Family {
    pub folder: &'static str,
    pub scope: Scope,
    pub kind: &'static str,
}

pub const KPROFILE_PODS_FAMILY: Family = Family {
    folder: "pods",
    scope: Scope::System,
    kind: "pod",
};
pub const KPROFILE_CONFIGS_FAMILY: Family = Family {
    folder: "configs",
    scope: Scope::Application,
    kind: "config",
};
pub const KPROFILE_ASSETS_FAMILY: Family = Family {
    folder: "assets",
    scope: Scope::Application,
    kind: "asset",
};

pub const REQUIREMENTS_TD_ASSET: &str = "requirements-td.txt";
pub const REQUIREMENTS_AG_ASSET: &str = "requirements-ag.txt";

pub const CUSTOMIZABLE_ASSETS: &[&str] = &[REQUIREMENTS_TD_ASSET, REQUIREMENTS_AG_ASSET];

pub const KIND_SECRET: &str = "secret";
pub const KIND_SECRET_CONFIG: &str = "secret-config";
pub const KIND_MANIFEST: &str = "manifest";

pub const SINGLETON_ID_TABSDATA: &str = "tabsdata";
pub const SINGLETON_NAME_MANIFEST: &str = "manifest.yaml";

pub const INIT_FOLDER: &str = "init";
pub const EDIT_FOLDER: &str = "edit";
pub const INPUT_FOLDER: &str = "input";
pub const STAGE_FOLDER: &str = "stage";
pub const TRASH_FOLDER: &str = "trash";
pub const SETTINGS_FILE: &str = ".settings.json";
pub const CLUSTER_FOLDER: &str = "cluster";
pub const KUBE_FOLDER: &str = "kube";
pub const CLUSTER_CONFIG_FOLDER: &str = "config";
pub const KUBECONFIG_FILE: &str = "kubeconfig.yaml";
pub const DEFAULT_NAMESPACE: &str = "default";
pub const VENV_ACTIVATE_FILE: &str = "activate";

pub const PROGRAM_TD_RESOURCE: &str = "td_resource";

pub const DEFAULT_INSTANCE_NAME: &str = "tabsdata";

pub const DATABASE_HOST_LOOPBACK: &str = "127.0.0.1";
pub const DATABASE_NAME_POSTGRES: &str = "postgres";
pub const YAML_NULL: &str = "null";
pub const DATABASE_SSL_MODE_PREFER: &str = "prefer";
pub const DATABASE_SSL_MODE_VERIFY_CA: &str = "verify-ca";
pub const CONTAINER_CERTS_ROOT: &str = "/tabsdata/certs";
pub const DATABASE_CA_BUNDLE_FILE: &str = "ca.pem";
pub const DATABASE_CA_BUNDLE_URL: &str =
    "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem";
pub const DATABASE_SSL_ROOT_CERT: &str =
    concatcp!(CONTAINER_CERTS_ROOT, "/", DATABASE_CA_BUNDLE_FILE);

pub const TABSDATA_LABEL_NAMESPACE: &str = "tabsdata.com/";
pub const KUBERNETES_LABEL_NAMESPACE: &str = "app.kubernetes.io/";

pub const TIER_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "tier");
pub const SEQUENCE_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "sequence");
pub const CONFIGMAP_FAMILY_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "configmap-family");
pub const SECRET_FAMILY_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "secret-family");
pub const SECRET_FAMILY_CONFIG: &str = "secret-config";

pub const SECRET_GENERATOR_JWT_SIGNING_KEY: &str = "jwt_signing_key";
pub const SECRET_GENERATOR_POSTGRES_SYSTEM_USER: &str = "postgres_system_user";
pub const SECRET_GENERATOR_POSTGRES_PASSWORD: &str = "postgres_password";
pub const SECRET_GENERATOR_APISERVER_SERVICE_TOKEN: &str = "apiserver_service_token";

pub const PART_OF_LABEL: &str = concatcp!(KUBERNETES_LABEL_NAMESPACE, "part-of");
pub const PART_OF_TABSDATA: &str = "tabsdata";

pub const POD_YAML_COMPONENT_LABEL: &str = concatcp!(KUBERNETES_LABEL_NAMESPACE, "component");
pub const POD_YAML_DECORATOR_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "decorator");
pub const POD_YAML_WORKER_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "worker");

pub const TABSDATA_NAME_PREFIX: &str = "td-";

pub const CONFIGMAP_TABSDATA_MANIFEST: &str = "td-sys-tabsdata-manifest-manifest.yaml";
pub const CONFIGMAP_TABSDATA_SETTINGS: &str = "td-sys-tabsdata-settings-settings.json";
pub const CONTAINER_ROOT_ENV: &str = "CONTAINER_ROOT";
pub const CONTAINER_TABSDATA_ROOT: &str = "/tabsdata";
pub const CONTAINER_SETTINGS_RELATIVE: &str = "tabsdata/settings/.settings.json";

pub const SECRET_CATEGORY_OPENAI_KEY: &str = "openai-key";
pub const SECRET_CATEGORY_ANTHROPIC_KEY: &str = "anthropic-key";
pub const SECRET_TABSDATA_KUBECONFIG: &str = "td-sys-tabsdata-secret-kubeconfig";
pub const SECRET_KEY_KUBECONFIG: &str = "kubeconfig";

pub const KUBE_NAME_SEPARATOR: &str = "-";
pub const KUBE_METADATA_FIELD_NAME: &str = "name";
pub const KUBE_METADATA_FIELD_UID: &str = "uid";

pub const HTTP_STATUS_FORBIDDEN: u16 = 403;
pub const HTTP_STATUS_NOT_FOUND: u16 = 404;
pub const HTTP_STATUS_CONFLICT: u16 = 409;

pub const SCHEDULER_FARGATE: &str = "fargate-scheduler";

pub const PROBE_POD: &str = "td-scheduler-probe";
pub const PROBE_IMAGE: &str = "busybox:1.37";
pub const PROBE_CPU: &str = "250m";
pub const PROBE_MEMORY: &str = "512Mi";

pub const POD_YAML_FIELD_KIND: &str = "kind";
pub const POD_YAML_FIELD_METADATA: &str = "metadata";
pub const POD_YAML_FIELD_LABELS: &str = "labels";

pub const VALUE_Y: &str = "y";
pub const VALUE_YES: &str = "yes";
pub const VALUE_N: &str = "n";
pub const VALUE_NO: &str = "no";

pub const DELETE_POD_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DELETE_POD_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub const POD_PHASE_PENDING: &str = "Pending";
pub const POD_PHASE_RUNNING: &str = "Running";
pub const POD_PHASE_SUCCEEDED: &str = "Succeeded";
pub const POD_PHASE_FAILED: &str = "Failed";

pub const LOG_POD_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub const KMINUS_MIN_NOFILE: u64 = 65536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[value(rename_all = "lower")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

pub const DEFAULT_LOG_LEVEL: LogLevel = LogLevel::Debug;

#[cfg(test)]
mod tests;
