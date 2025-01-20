//
// Copyright 2024 Tabs Data Inc.
//

use crate::descriptor::GIT_FOLDER;
use duct::cmd;
use std::path::PathBuf;

pub fn find_workspace_root() -> PathBuf {
    let mut current_dir = option_env!("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let command = if cfg!(windows) {
                cmd!("cmd", "/C", "cd")
            } else {
                cmd!("pwd")
            };
            let current_path = command
                .read()
                .unwrap_or_else(|_| "Failed to get current directory".to_string());
            PathBuf::from(current_path)
        });
    loop {
        let git_dir = current_dir.join(GIT_FOLDER);
        if git_dir.exists() && git_dir.is_dir() {
            return current_dir;
        }
        if !current_dir.pop() {
            panic!("No .git directory found in the project hierarchy");
        }
    }
}
