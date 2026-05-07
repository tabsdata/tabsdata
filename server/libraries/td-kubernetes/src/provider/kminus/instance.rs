//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use td_common::status::ExitStatus::GeneralError;

use crate::cerror;
use crate::constants::{CONFIGMAP_TABSDATA, FOLDER_PROFILE};
use crate::kubernetes::action::{configmap_exists, create_configmap, delete_configmap};
use crate::kubernetes::client::build_kube_client;
use crate::kubernetes::kubeconfig::build_kubeconfig;
use crate::provider::Instance;
use crate::system::exit::{Result, abort};

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

    fn read_staged_profile(&self) -> Result<Option<BTreeMap<String, String>>> {
        let staged_folder = self.staging_profile_path();
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

    async fn replace_configmap(&self, data: BTreeMap<String, String>) -> Result {
        let kminus_config = build_kubeconfig(Some(&self.kubefile), None, None).await?;
        let client = build_kube_client(&kminus_config)?;
        let namespace = &kminus_config.default_namespace;
        if configmap_exists(&client, namespace, CONFIGMAP_TABSDATA).await? {
            delete_configmap(&client, namespace, CONFIGMAP_TABSDATA).await?;
        }
        create_configmap(&client, namespace, CONFIGMAP_TABSDATA, data).await
    }
}

impl Instance for KminusInstance {
    async fn start(&self) -> Result {
        let staged = self.read_staged_profile()?;
        let Some(data) = staged else {
            return Ok(());
        };
        self.replace_configmap(data).await?;
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
