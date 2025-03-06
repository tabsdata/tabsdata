//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use std::env;
use std::path::PathBuf;

const OUT_DIR: &str = "OUT_DIR";
const ROOT_PROJECT_FOLDER: &str = "ROOT_PROJECT_FOLDER";
const CARGO_MANIFEST_DIR: &str = "CARGO_MANIFEST_DIR";

const GIT_FOLDER: &str = ".git";
const CARGO_FILE: &str = "Cargo.toml";

#[proc_macro]
pub fn workspace_root(_: TokenStream) -> TokenStream {
    if let Some(workspace_root) = get_workspace_root() {
        println!("cargo:warning=workspace_root set to {:?}", workspace_root);
        let workspace_root = workspace_root.to_str().unwrap();
        let expanded = quote! {
            #workspace_root
        };
        TokenStream::from(expanded)
    } else {
        panic!("Unable to determine the workspace root. Compilation cannot proceed...");
    }
}

fn get_workspace_root() -> Option<PathBuf> {
    let mut build_root = PathBuf::from(
        env::var(OUT_DIR)
            .or_else(|_| env::var(ROOT_PROJECT_FOLDER))
            .or_else(|_| env::var(CARGO_MANIFEST_DIR))
            .unwrap_or_else(|_| {
                let env_vars = std::env::vars()
                    .map(|(k, v)| format!("- {}={}", k, v))
                    .collect::<Vec<String>>()
                    .join("\n");
                panic!(
                    "Neither OUT_DIR \
                    nor ROOT_PROJECT_FOLDER \
                    nor CARGO_MANIFEST_DIR is set. \
                    Compilation cannot proceed...\n{env_vars}"
                );
            }),
    );
    let mut cargo_folder: Option<PathBuf> = None;
    loop {
        let git_folder = build_root.join(GIT_FOLDER);
        if git_folder.exists() && git_folder.is_dir() {
            return Some(build_root.clone());
        }
        let cargo_file = build_root.join(CARGO_FILE);
        if cargo_file.exists() && cargo_file.is_file() {
            cargo_folder = Some(build_root.clone());
        }
        if !build_root.pop() {
            return cargo_folder;
        }
    }
}
