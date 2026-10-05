//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::base::constants::{
    Family, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
};
use crate::base::error::upgrade::UpgradeError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::core::system::exit::CommandResult;
use crate::kube::api::entity::configmap::{
    delete_configmap, exists_configmap, get_configmap, upsert_configmap,
};
use crate::kube::context::KubeContext;
use crate::runtime::instance::upgrade::plan::Step;

#[async_trait]
pub trait Store {
    fn locate(&self, step: &Step) -> CommandResult<String>;

    async fn read(&self, step: &Step) -> CommandResult<Option<String>>;

    async fn write(&self, step: &Step, content: String) -> CommandResult;

    async fn remove(&self, step: &Step) -> CommandResult;
}

pub fn family_of(folder: &str) -> CommandResult<&'static Family> {
    for family in [
        &KPROFILE_ASSETS_FAMILY,
        &KPROFILE_CONFIGS_FAMILY,
        &KPROFILE_PODS_FAMILY,
    ] {
        if family.folder == folder {
            return Ok(family);
        }
    }
    Err(UpgradeError::UnknownFamily {
        folder: folder.to_owned(),
    }
    .into())
}

pub struct ConfigmapStore<'a> {
    context: &'a KubeContext,
    version: String,
}

impl<'a> ConfigmapStore<'a> {
    pub fn new(context: &'a KubeContext, version: &str) -> Self {
        Self {
            context,
            version: version.to_owned(),
        }
    }
}

#[async_trait]
impl Store for ConfigmapStore<'_> {
    fn locate(&self, step: &Step) -> CommandResult<String> {
        let family = family_of(step.folder)?;
        Ok(to_kube_resource_name(
            family.scope,
            id_from_file(&step.file),
            family.kind,
            &step.file,
        )?)
    }

    async fn read(&self, step: &Step) -> CommandResult<Option<String>> {
        let name = self.locate(step)?;
        let Some(data) = get_configmap(self.context, &name).await? else {
            return Ok(None);
        };
        Ok(data.get(&step.file).cloned())
    }

    async fn write(&self, step: &Step, content: String) -> CommandResult {
        let family = family_of(step.folder)?;
        let name = self.locate(step)?;
        let mut data = BTreeMap::new();
        data.insert(step.file.clone(), content);
        upsert_configmap(
            self.context,
            &name,
            data,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            Some(family.kind),
            Some(&self.version),
        )
        .await?;
        Ok(())
    }

    async fn remove(&self, step: &Step) -> CommandResult {
        let name = self.locate(step)?;
        if exists_configmap(self.context, &name).await? {
            delete_configmap(self.context, &name).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Keying {
    Variant,
    File,
}

pub struct FolderStore {
    folder: PathBuf,
    keying: Keying,
}

impl FolderStore {
    pub fn new(kprofile_folder: &Path) -> Self {
        Self {
            folder: kprofile_folder.to_path_buf(),
            keying: Keying::Variant,
        }
    }

    pub fn resolved(folder: &Path) -> Self {
        Self {
            folder: folder.to_path_buf(),
            keying: Keying::File,
        }
    }

    fn path_of(&self, step: &Step) -> PathBuf {
        let name = match self.keying {
            Keying::Variant => &step.variant,
            Keying::File => &step.file,
        };
        self.folder.join(step.folder).join(name)
    }
}

#[async_trait]
impl Store for FolderStore {
    fn locate(&self, step: &Step) -> CommandResult<String> {
        Ok(self.path_of(step).display().to_string())
    }

    async fn read(&self, step: &Step) -> CommandResult<Option<String>> {
        let path = self.path_of(step);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path).map_err(|error| {
            UpgradeError::OfflineSourceReadFailed {
                path: path.display().to_string(),
                cause: error.to_string(),
            }
        })?;
        Ok(Some(content))
    }

    async fn write(&self, step: &Step, content: String) -> CommandResult {
        let path = self.path_of(step);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                UpgradeError::OfflineTargetWriteFailed {
                    path: parent.display().to_string(),
                    cause: error.to_string(),
                }
            })?;
        }
        std::fs::write(&path, content).map_err(|error| UpgradeError::OfflineTargetWriteFailed {
            path: path.display().to_string(),
            cause: error.to_string(),
        })?;
        Ok(())
    }

    async fn remove(&self, step: &Step) -> CommandResult {
        let path = self.path_of(step);
        if !path.exists() {
            return Ok(());
        }
        std::fs::remove_file(&path).map_err(|error| UpgradeError::OfflineTargetWriteFailed {
            path: path.display().to_string(),
            cause: error.to_string(),
        })?;
        Ok(())
    }
}
