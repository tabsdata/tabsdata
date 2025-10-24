//
// Copyright 2024 Tabs Data Inc.
//

use std::path::PathBuf;
use td_build::customizer::{Customization, Customizer};
use tm_workspace::workspace_root;

fn main() {
    const TABSDATA_SOLUTION_HOME: &str = "TABSDATA_SOLUTION_HOME";

    let tabsdata_solution_home = std::env::var(TABSDATA_SOLUTION_HOME)
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let root = PathBuf::from(workspace_root!());
            root.parent()
                .expect("Workspace root must have a parent directory")
                .to_path_buf()
        });
    let path_before_canonicalize = tabsdata_solution_home.clone();
    let tabsdata_solution_home = tabsdata_solution_home.canonicalize().unwrap_or_else(|e| {
        panic!(
            "Failed to canonicalize tabsdata solution home - '{:?}': {}",
            path_before_canonicalize, e
        )
    });
    println!(
        "cargo:warning=tabsdata solution home: {:?}",
        tabsdata_solution_home
    );

    match std::fs::read_dir(&tabsdata_solution_home) {
        Ok(entries) => {
            println!("cargo:warning=🪣 Contents of tabsdata solution home:");
            let mut items: Vec<_> = entries.filter_map(|e| e.ok()).collect();
            items.sort_by_key(|e| e.path());
            for entry in items {
                let path = entry.path();
                let file_type = if path.is_dir() { "folder" } else { "file" };
                println!(
                    "cargo:warning=   📚 [{:6}] {}",
                    file_type,
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
            }
        }
        Err(error) => {
            panic!(
                "Failed to read contents of tabsdata solution home: {}",
                error
            );
        }
    }

    Customization::customize();
}
