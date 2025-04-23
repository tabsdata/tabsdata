//
// Copyright 2025 Tabs Data Inc.
//

//! File test utility functions.

use std::path::PathBuf;

pub fn dummy_file() -> String {
    if cfg!(target_os = "windows") {
        "file:///c:/dummy".to_string()
    } else {
        "file:///dummy".to_string()
    }
}

pub fn mount_uri(test_dir: impl Into<PathBuf>) -> String {
    let test_dir = test_dir.into();
    if cfg!(target_os = "windows") {
        format!("file:///{}", test_dir.to_string_lossy())
    } else {
        format!("file://{}", test_dir.to_string_lossy())
    }
}
