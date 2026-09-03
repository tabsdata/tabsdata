//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::io::ErrorKind;
use std::path::Path;

pub fn is_symlink(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|metadata| metadata.is_symlink())
        .unwrap_or(false)
}

pub fn relink(link: &Path, target: &Path) -> Result<(), String> {
    match fs::symlink_metadata(link) {
        Ok(metadata) if metadata.is_symlink() => {
            remove_symlink(link, &metadata).map_err(|error| error.to_string())?;
        }
        Ok(metadata) if metadata.is_dir() => {
            fs::remove_dir_all(link).map_err(|error| error.to_string())?;
        }
        Ok(_) => {
            fs::remove_file(link).map_err(|error| error.to_string())?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }
    symlink_folder(target, link).map_err(|error| error.to_string())
}

#[cfg(unix)]
fn remove_symlink(link: &Path, _metadata: &fs::Metadata) -> std::io::Result<()> {
    fs::remove_file(link)
}

#[cfg(windows)]
fn remove_symlink(link: &Path, metadata: &fs::Metadata) -> std::io::Result<()> {
    use std::os::windows::fs::FileTypeExt;

    if metadata.file_type().is_symlink_dir() {
        fs::remove_dir(link)
    } else {
        fs::remove_file(link)
    }
}

#[cfg(unix)]
pub fn symlink_folder(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
pub fn symlink_folder(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[cfg(test)]
mod tests;
