//
// Copyright 2026 Tabs Data Inc.
//

use std::time::Duration;

use const_format::concatcp;

pub const ENV_KUBECONFIG: &str = "KUBECONFIG";

pub const FOLDER_CLUSTER: &str = "cluster";
pub const FOLDER_REPOSITORY: &str = "repository";
pub const FOLDER_DATABASE: &str = "database";
pub const FOLDER_STORAGE: &str = "storage";
pub const FOLDER_RUNTIMES: &str = "runtimes";
pub const FOLDER_IMAGES: &str = "images";
pub const FOLDER_PYTHON: &str = "python";
pub const FOLDER_PYENV_TD: &str = "td";
pub const FOLDER_PYENV_AG: &str = "ag";
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

pub const TABSDATA_LABEL_NAMESPACE: &str = "tabsdata.com/";
pub const KUBERNETES_LABEL_NAMESPACE: &str = "app.kubernetes.io/";

pub const TIER_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "tier");
pub const CONFIGMAP_FAMILY_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "configmap-family");
pub const SECRET_FAMILY_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "secret-family");
pub const SECRET_FAMILY_CONFIG: &str = "secret-config";

pub const SECRET_GENERATOR_JWT_SIGNING_KEY: &str = "jwt_signing_key";
pub const SECRET_GENERATOR_POSTGRES_SYSTEM_USER: &str = "postgres_system_user";
pub const SECRET_GENERATOR_POSTGRES_PASSWORD: &str = "postgres_password";

pub const PART_OF_LABEL: &str = concatcp!(KUBERNETES_LABEL_NAMESPACE, "part-of");
pub const PART_OF_TABSDATA: &str = "tabsdata";

pub const POD_YAML_COMPONENT_LABEL: &str = concatcp!(KUBERNETES_LABEL_NAMESPACE, "component");
pub const POD_YAML_DECORATOR_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "decorator");
pub const POD_YAML_WORKER_LABEL: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "worker");

pub const TABSDATA_NAME_PREFIX: &str = "td-";

pub const CONFIGMAP_TABSDATA_MANIFEST: &str = "td-sys-tabsdata-manifest-manifest.yaml";
pub const CONFIGMAP_TABSDATA_SETTINGS: &str = "td-sys-tabsdata-settings-settings.json";
pub const CONTAINER_ROOT_ENV: &str = "CONTAINER_ROOT";
pub const CONTAINER_SETTINGS_RELATIVE: &str = "tabsdata/settings/.settings.json";

pub const SECRET_CATEGORY_OPENAI_KEY: &str = "openai-key";
pub const SECRET_CATEGORY_ANTHROPIC_KEY: &str = "anthropic-key";
pub const SECRET_TABSDATA_KUBECONFIG: &str = "td-sys-tabsdata-secret-kubeconfig";
pub const SECRET_KEY_KUBECONFIG: &str = "kubeconfig";

pub const KUBE_NAME_SEPARATOR: &str = "-";
pub const KUBE_METADATA_FIELD_NAME: &str = "name";
pub const KUBE_METADATA_FIELD_UID: &str = "uid";

pub const HTTP_STATUS_CONFLICT: u16 = 409;

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
