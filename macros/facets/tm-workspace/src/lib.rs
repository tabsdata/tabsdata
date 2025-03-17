//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::path::PathBuf;
use std::{env, fs};

const OUT_DIR: &str = "OUT_DIR";
const ROOT_PROJECT_FOLDER: &str = "ROOT_PROJECT_FOLDER";
const CARGO_MANIFEST_DIR: &str = "CARGO_MANIFEST_DIR";

const GIT_FOLDER: &str = ".git";
const CARGO_FILE: &str = "Cargo.toml";
const ROOT_FILE: &str = ".root";

const LOG_CRATE_TM_WORKSPACE: Option<&str> = option_env!("LOG_CRATE_TM_WORKSPACE");

#[proc_macro]
pub fn workspace_root(_: TokenStream) -> TokenStream {
    let id = random_string(8);
    let log_crate_tm_workspace = LOG_CRATE_TM_WORKSPACE.unwrap_or("true") == "true";

    if let Some(workspace_root) = get_workspace_root(id.clone(), log_crate_tm_workspace) {
        if log_crate_tm_workspace {
            eprintln!(
                "📒 - {} · Setting workspace_root to {:?}",
                id, workspace_root
            );
        }
        let workspace_root = workspace_root.to_str().unwrap();
        let expanded = quote! {
            #workspace_root
        };
        TokenStream::from(expanded)
    } else {
        panic!(
            "📕 - {} · Unable to determine the workspace root. Compilation cannot proceed...",
            id
        );
    }
}

fn get_workspace_root(id: String, log_crate_tm_workspace: bool) -> Option<PathBuf> {
    let env_vars = [OUT_DIR, ROOT_PROJECT_FOLDER, CARGO_MANIFEST_DIR];
    let mut build_roots: Vec<PathBuf> = vec![];
    for var in env_vars {
        if let Ok(value) = env::var(var) {
            let path = PathBuf::from(value);
            if log_crate_tm_workspace {
                eprintln!(
                    "📗 {} · Found environment variable {} = {:?}",
                    id, var, path
                );
            }
            if path.exists() {
                build_roots.push(path);
            } else if log_crate_tm_workspace {
                eprintln!(
                    "📙 {} · Path from environment variable {} does not exist: {:?}",
                    id, var, path
                );
            }
        } else if log_crate_tm_workspace {
            eprintln!("📙 {} · Environment variable {} is not set", id, var);
        }
    }
    if build_roots.is_empty() {
        let env_dump = env::vars()
            .map(|(k, v)| format!("- {}={}", k, v))
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "📕 - {} · \
            Neither OUT_DIR \
            nor ROOT_PROJECT_FOLDER \
            nor CARGO_MANIFEST_DIR is set. \
            Compilation cannot proceed...\n{}",
            id, env_dump
        );
    }

    let mut cargo_folder: Option<PathBuf> = None;
    for mut build_root in build_roots {
        loop {
            if log_crate_tm_workspace {
                eprintln!("📗 {} · Exploring folder: {:?}", id, build_root);
            }
            match fs::read_dir(&build_root) {
                Ok(entries) => {
                    for entry in entries.filter_map(Result::ok) {
                        let path = entry.path();
                        let file = path.file_name().unwrap_or_default().to_string_lossy();
                        if log_crate_tm_workspace {
                            if file == ROOT_FILE || file == GIT_FOLDER || file == CARGO_FILE {
                                eprintln!("   🔥 - {} · Entry: {:?}", id, path);
                            } else if log_crate_tm_workspace {
                                eprintln!("   📘 - {} · Entry: {:?}", id, path);
                            }
                        }
                    }
                }
                Err(e) => {
                    if log_crate_tm_workspace {
                        eprintln!(
                            "📕 - {} · Failed to read directory {:?}: {}",
                            id, build_root, e
                        );
                    }
                }
            }

            let root_file = build_root.join(ROOT_FILE);
            if root_file.exists() && root_file.is_file() {
                return Some(build_root.clone());
            }
            let git_folder = build_root.join(GIT_FOLDER);
            if git_folder.exists() && git_folder.is_dir() {
                return Some(build_root.clone());
            }
            let cargo_file = build_root.join(CARGO_FILE);
            if cargo_file.exists() && cargo_file.is_file() {
                cargo_folder = Some(build_root.clone());
            }
            if !build_root.pop() {
                break;
            }
        }
    }
    cargo_folder
}

fn random_string(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
