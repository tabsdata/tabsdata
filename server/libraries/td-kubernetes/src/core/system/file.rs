//
// Copyright 2026 Tabs Data Inc.
//

use std::path::Path;

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
