//
// Copyright 2024 Tabs Data Inc.
//

use crate::services::tdserver::TD_KEEP;
use include_dir::{Dir, DirEntry, include_dir};
use std::fs::{File, create_dir_all, write};
use std::io::{Result, Write};
use std::path::{Path, PathBuf};
use td_common::env::TABSDATA_HOME_DIR;
use td_common::files::ROOT;
use td_common::server::{
    CONFIG_FILE, CONFIG_FOLDER, EXCLUSION_PREFIX, INSTANCES_FOLDER, WORKSPACE_FOLDER,
};
use td_common::settings::{DEFAULT_SETTINGS, SETTINGS_FILE};
use tracing::trace;

pub static PROFILE: Dir<'_> = include_dir!("variant/resources/profile");

pub fn extract_profile<P: AsRef<Path>>(destination: P, rewrite: bool) -> Result<()> {
    extract_folder(destination, &PROFILE, rewrite)
}

pub fn extract_profile_config<P: AsRef<Path>>(destination: P) -> Result<Option<PathBuf>> {
    let config_yaml = Path::new(WORKSPACE_FOLDER)
        .join(CONFIG_FOLDER)
        .join(CONFIG_FILE);
    let result = extract_file(&destination, &PROFILE, config_yaml.as_path());
    match result {
        Ok(_) => Ok(Some(destination.as_ref().join(config_yaml))),
        Err(err) => Err(err),
    }
}

fn extract_folder<P: AsRef<Path>>(destination: P, resource: &Dir, rewrite: bool) -> Result<()> {
    for entry in resource.entries() {
        match entry {
            DirEntry::File(file) => {
                let path = file.path();
                if !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(EXCLUSION_PREFIX)
                    || path.file_name().unwrap() == TD_KEEP
                {
                    trace!("Reading resource file: '{:?}'", path);
                    let path = destination.as_ref().join(path);
                    if let Some(parent) = path.parent() {
                        create_dir_all(parent)?;
                    }
                    if !&path.exists() || rewrite {
                        write(&path, file.contents())?;
                    }
                }
            }
            DirEntry::Dir(folder) => {
                let path = folder.path();
                if !path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(EXCLUSION_PREFIX)
                {
                    trace!("Reading resource folder: '{:?}'", folder.path());
                    create_dir_all(destination.as_ref().join(folder.path()))?;
                    extract_folder(destination.as_ref(), folder, rewrite)?;
                }
            }
        }
    }
    Ok(())
}

fn extract_file<P: AsRef<Path>>(destination: P, resource: &Dir, target: &Path) -> Result<()> {
    for entry in resource.entries() {
        match entry {
            DirEntry::File(file) => {
                let path = file.path();
                if path == target {
                    let path = destination.as_ref().join(path);
                    if let Some(parent) = path.parent() {
                        create_dir_all(parent)?;
                    }
                    write(&path, file.contents())?;
                    break;
                }
            }
            DirEntry::Dir(folder) => {
                extract_file(destination.as_ref(), folder, target)?;
            }
        }
    }
    Ok(())
}

pub fn extract_default_settings<P: AsRef<Path>>(
    instance: Option<String>,
    destination: Option<P>,
) -> Result<PathBuf> {
    let root = dirs::home_dir().unwrap_or_else(|| PathBuf::from(ROOT));
    let folder = match destination.as_ref().map(|p| p.as_ref()) {
        None => root.join(TABSDATA_HOME_DIR),
        Some(path) if path.is_relative() => root
            .join(TABSDATA_HOME_DIR)
            .join(INSTANCES_FOLDER)
            .join(path),
        Some(path) => path.to_path_buf(),
    };
    create_dir_all(&folder)?;
    let file_name = match instance {
        None => SETTINGS_FILE.to_string(),
        Some(name) => format!("settings_{name}.yaml"),
    };
    let settings_path = folder.join(file_name);
    let mut settings_file = File::create(&settings_path)?;
    settings_file.write_all(DEFAULT_SETTINGS.as_bytes())?;
    settings_file.flush()?;

    Ok(settings_path)
}
