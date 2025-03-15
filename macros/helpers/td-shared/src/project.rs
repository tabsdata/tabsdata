//
// Copyright 2024 Tabs Data Inc.
//

use duct::cmd;
use std::path::PathBuf;

const GIT_FOLDER: &str = ".git";
pub const ROOT_FILE: &str = ".root";
const CARGO_FILE: &str = "Cargo.toml";

pub fn get_project_root() -> String {
    /* Function current_dir() is undesired in this case, as, in Linux & macOS systems, this gets resolved
       to getcwd() function, which, as per the POSIX spec, The pathname shall contain no components that
       are dot or dot-dot, or are symbolic links.
    */
    let current_folder = if let Some(pwd) = option_env!("CARGO_MANIFEST_DIR") {
        pwd.to_string()
    } else {
        let command = if cfg!(windows) {
            cmd!("cmd", "/C", "cd")
        } else {
            cmd!("pwd")
        };
        command
            .read()
            .unwrap_or_else(|_| "Failed to get current directory".to_string())
    };
    let mut current_dir = PathBuf::from(current_folder);
    let mut cargo_dir: Option<PathBuf> = None;
    loop {
        let root_file = current_dir.join(ROOT_FILE);
        if root_file.exists() && root_file.is_file() {
            return current_dir.as_os_str().to_string_lossy().to_string();
        }
        let git_dir = current_dir.join(GIT_FOLDER);
        if git_dir.exists() && git_dir.is_dir() {
            return current_dir.as_os_str().to_string_lossy().to_string();
        }

        let cargo_file = current_dir.join(CARGO_FILE);
        if cargo_file.exists() && cargo_file.is_file() {
            cargo_dir = Some(current_dir.clone());
        }

        if !current_dir.pop() {
            break;
        }
    }
    if let Some(cargo_dir) = cargo_dir {
        return cargo_dir.as_os_str().to_string_lossy().to_string();
    }
    panic!("No .git directory found in the project hierarchy");
}
