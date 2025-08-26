//
// Copyright 2024 Tabs Data Inc.
//

use crate::server::EXCLUSION_PREFIX;
use std::fs;
#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

#[cfg(not(target_os = "windows"))]
pub const ROOT: &str = "/";
#[cfg(target_os = "windows")]
pub const ROOT: &str = "c:\\";

pub const YAML_EXTENSION: &str = "yaml";
pub const LOCK_EXTENSION: &str = "lock";

pub fn get_files_in_folder_sorted_by_name<P: AsRef<Path>>(
    folder: P,
    extension: Option<&str>,
) -> std::io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(folder)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() {
                if let Some(token) = extension
                    && path.extension().and_then(|e| e.to_str()) != Some(token)
                {
                    return None;
                }
                if let Some(file_name) = path.file_name().and_then(|name| name.to_str())
                    && !file_name.starts_with(EXCLUSION_PREFIX)
                {
                    return Some(path);
                }
            }
            None
        })
        .collect();
    files.sort();
    Ok(files)
}

pub fn get_files_in_subfolders_sorted_by_name<P: AsRef<Path>>(
    folder: P,
    subfolder: String,
    extension: String,
) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(folder) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let subentry = entry.path().join(&subfolder);
                if subentry.is_dir()
                    && let Ok(subfiles) = fs::read_dir(&subentry)
                {
                    for subfile in subfiles.flatten() {
                        if let Some(filetype) = subfile.path().extension()
                            && filetype.to_string_lossy() == extension
                        {
                            files.push(subfile.path());
                        }
                    }
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(windows)]
pub fn make_executable(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(windows))]
pub fn make_executable(path: &Path) -> std::io::Result<()> {
    let metadata = fs::metadata(path)?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o777);
    fs::set_permissions(path, permissions)?;
    Ok(())
}
