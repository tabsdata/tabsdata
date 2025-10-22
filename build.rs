//
// Copyright 2024 Tabs Data Inc.
//

use crate::boot::boot;
use std::path::PathBuf;
use tm_workspace::workspace_root;

mod boot;

const TABSDATA_SOLUTION_HOME: &str = "TABSDATA_SOLUTION_HOME";

fn main() {
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
        "cargo:info=tabsdata solution home: {:?}",
        tabsdata_solution_home
    );

    boot();
}
