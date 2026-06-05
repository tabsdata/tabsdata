//
// Copyright 2026 Tabs Data Inc.
//

use std::fs;
use std::path::Path;

use walkdir::WalkDir;

pub fn is_hidden_or_system(path: &Path) -> bool {
    let dot_prefixed = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with('.'))
        .unwrap_or(false);
    dot_prefixed || is_os_hidden(path)
}

#[cfg(windows)]
fn is_os_hidden(path: &Path) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
    std::fs::metadata(path)
        .map(|metadata| metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN != 0)
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn is_os_hidden(_path: &Path) -> bool {
    false
}

pub const IGNORE_MARKER: &str = ".ignore";

pub fn copy_tree(source: &Path, target: &Path) -> Result<(), String> {
    fs::create_dir_all(target).map_err(|error| error.to_string())?;
    let walker = WalkDir::new(source)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            entry.path() == source
                || (!is_hidden_or_system(entry.path()) && !has_ignore_marker(entry.path()))
        });
    for entry in walker {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_path = entry.path();
        let relative = entry_path
            .strip_prefix(source)
            .map_err(|error| error.to_string())?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let destination = target.join(relative);
        let file_type = entry.file_type();
        if file_type.is_dir() {
            fs::create_dir_all(&destination).map_err(|error| error.to_string())?;
        } else if file_type.is_file() {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            fs::copy(entry_path, &destination).map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn has_ignore_marker(path: &Path) -> bool {
    path.is_dir() && path.join(IGNORE_MARKER).exists()
}
