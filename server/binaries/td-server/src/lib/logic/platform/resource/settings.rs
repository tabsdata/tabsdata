//
// Copyright 2024 Tabs Data Inc.
//

use crate::bin::tdserver::TD_KEEP;
use crate::logic::platform::resource::instance::{CONFIG_FILE, CONFIG_FOLDER, WORKSPACE_FOLDER};
use include_dir::{include_dir, Dir, DirEntry};
use std::fs::{create_dir_all, write};
use std::io::Result;
use std::path::{Path, PathBuf};
use td_common::server::EXCLUSION_PREFIX;
use tracing::trace;

pub static DEFAULT_DOMAIN: Dir<'_> =
    include_dir!("server/binaries/td-server/resources/profiles/internal/default");

pub fn extract_domain<P: AsRef<Path>>(destination: P, rewrite: bool) -> Result<()> {
    extract_folder(destination, &DEFAULT_DOMAIN, rewrite)
}

pub fn extract_domain_config<P: AsRef<Path>>(destination: P) -> Result<Option<PathBuf>> {
    let config_yaml = Path::new(WORKSPACE_FOLDER)
        .join(CONFIG_FOLDER)
        .join(CONFIG_FILE);
    let result = extract_file(&destination, &DEFAULT_DOMAIN, config_yaml.as_path());
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
