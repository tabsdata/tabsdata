//
// Copyright 2024 Tabs Data Inc.
//

use std::fs;
use td_build::structure::find_workspace_root;
use toml::Value;

const GIT_FOLDER: &str = ".git";
const CARGO_TOML_FILE: &str = "Cargo.toml";

const TAG_WORKSPACE: &str = "workspace";
const TAG_DEPENDENCIES: &str = "dependencies";
const TAG_VERSION: &str = "version";
const DEPENDENCY_OBJECT_STORE: &str = "object_store";

pub struct Boot;

pub trait Loader {
    fn load() {}
}

impl Loader for Boot {
    fn load() {
        set_rust_environment();
    }
}

fn set_rust_environment() {
    let root_folder = find_workspace_root();
    let git_dir = root_folder.join(GIT_FOLDER);
    if git_dir.exists() && git_dir.is_dir() {
        let cargo_toml_file = root_folder.join(CARGO_TOML_FILE);
        if cargo_toml_file.exists() {
            let cargo_toml_content =
                fs::read_to_string(&cargo_toml_file).expect("Unable to read Cargo.toml file");
            let cargo_toml: Value = cargo_toml_content
                .parse::<Value>()
                .expect("Unable to parse Cargo.toml file");
            let object_store_version = cargo_toml
                .get(TAG_WORKSPACE)
                .and_then(|workspace| workspace.get(TAG_DEPENDENCIES))
                .and_then(|deps| deps.get(DEPENDENCY_OBJECT_STORE))
                .and_then(|dep| dep.get(TAG_VERSION))
                .and_then(|v| v.as_str())
                .expect("Unable to find object_store version");
            println!(
                "cargo:rustc-env=OBJECT_STORE_VERSION={}",
                object_store_version
            );
        } else {
            panic!(
                "No .git folder found in the project root folder: {:?}",
                root_folder
            );
        }
    } else {
        panic!(
            "No Cargo.toml file found in the project root folder: {:?}",
            root_folder
        );
    }
}
