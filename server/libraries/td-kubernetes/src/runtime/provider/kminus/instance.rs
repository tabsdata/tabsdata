//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use td_common::status::ExitStatus::{GeneralError, TabsDataError};
use tracing::info;

use crate::base::constants::{
    CONFIGMAP_TABSDATA_CONFIGS, CONFIGMAP_TABSDATA_GLOBALS, CONFIGMAP_TABSDATA_PODS,
    FOLDER_PROFILE, KPROFILE_CONFIGS_FOLDER, KPROFILE_GLOBALS_FOLDER, KPROFILE_PODS_FOLDER,
};
use crate::cerror;
use std::sync::Arc;

use crate::core::system::exit::{Result, abort};
use crate::kube::api::{create_configmap, delete_configmap, exists_configmap};
use crate::kube::context::KubeContext;
use crate::kube::kubeconfig::build_kubeconfig;
use crate::runtime::instance::profile::RenderedConfigmap;
use crate::runtime::provider::Instance;
use crate::runtime::reporting::build_kube_client;

pub struct KminusInstance {
    kubefile: PathBuf,
    staging_path: PathBuf,
}

impl KminusInstance {
    pub fn new(kubefile: impl Into<PathBuf>, staging_path: impl Into<PathBuf>) -> Self {
        Self {
            kubefile: kubefile.into(),
            staging_path: staging_path.into(),
        }
    }

    fn staging_profile_path(&self) -> PathBuf {
        self.staging_path.join(FOLDER_PROFILE)
    }

    fn read_staged_profile(&self) -> Result<Option<StagedConfigmaps>> {
        let staged_folder = self.staging_profile_path();
        if !staged_folder.is_dir() {
            return Ok(None);
        }
        let globals = read_staged_folder(&staged_folder, KPROFILE_GLOBALS_FOLDER)?;
        let pods = read_staged_folder(&staged_folder, KPROFILE_PODS_FOLDER)?;
        let configs = read_staged_folder(&staged_folder, KPROFILE_CONFIGS_FOLDER)?;
        if globals.is_empty() && pods.is_empty() && configs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(StagedConfigmaps {
                globals,
                pods,
                configs,
            }))
        }
    }

    async fn replace_configmaps(&self, staged: StagedConfigmaps) -> Result {
        let kminus_config = build_kubeconfig(Some(&self.kubefile), None, None).await?;
        let namespace = kminus_config.default_namespace.clone();
        let client = build_kube_client(&kminus_config)?;
        let context = KubeContext::new(Arc::new(client), namespace);
        for (name, data) in [
            (CONFIGMAP_TABSDATA_GLOBALS, staged.globals),
            (CONFIGMAP_TABSDATA_PODS, staged.pods),
            (CONFIGMAP_TABSDATA_CONFIGS, staged.configs),
        ] {
            if exists_configmap(&context, name).await? {
                delete_configmap(&context, name).await?;
            }
            create_configmap(&context, name, data).await?;
        }
        Ok(())
    }
}

struct StagedConfigmaps {
    globals: RenderedConfigmap,
    pods: RenderedConfigmap,
    configs: RenderedConfigmap,
}

fn read_staged_folder(staged_folder: &Path, folder: &str) -> Result<RenderedConfigmap> {
    let folder_path = staged_folder.join(folder);
    let mut data = BTreeMap::new();
    if !folder_path.is_dir() {
        return Ok(data);
    }
    let read = match fs::read_dir(&folder_path) {
        Ok(entries) => entries,
        Err(error) => {
            cerror!(
                "Failed to read staged folder '{}': {error}",
                folder_path.display()
            );
            return abort(GeneralError);
        }
    };
    for entry in read {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                cerror!("Failed to enumerate '{}': {error}", folder_path.display());
                return abort(GeneralError);
            }
        };
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with('.') || is_hidden(&path) {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                cerror!("Failed to stat '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        if file_type.is_dir() {
            cerror!(
                "Staged profile folder '{folder}' must be flat, but contains the \
                 subfolder '{name}'."
            );
            return abort(TabsDataError);
        }
        if !file_type.is_file() {
            continue;
        }
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                info!(
                    "Skipping non-UTF-8 file in staging folder: '{}'",
                    path.display()
                );
                continue;
            }
            Err(error) => {
                cerror!("Failed to read '{}': {error}", path.display());
                return abort(GeneralError);
            }
        };
        data.insert(name.to_owned(), contents);
    }
    Ok(data)
}

impl Instance for KminusInstance {
    async fn start(&self) -> Result {
        let staged = self.read_staged_profile()?;
        let Some(staged) = staged else {
            return Ok(());
        };
        self.replace_configmaps(staged).await?;
        if let Err(error) = fs::remove_dir_all(&self.staging_path) {
            cerror!(
                "Failed to remove staging folder '{}': {error}",
                self.staging_path.display()
            );
            return abort(GeneralError);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<bool> {
        Ok(false)
    }

    async fn is_alive(&self) -> bool {
        false
    }
}

#[cfg(windows)]
fn is_hidden(path: &Path) -> bool {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
    fs::metadata(path)
        .map(|metadata| metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN != 0)
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn is_hidden(_path: &Path) -> bool {
    false
}
