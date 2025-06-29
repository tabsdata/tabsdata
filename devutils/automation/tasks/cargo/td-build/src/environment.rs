//
// Copyright 2025 Tabs Data Inc.
//

use crate::structure::find_workspace_root;
use std::error::Error;
use std::{env, fs};

const ENV_TD_UI_MODE: &str = "TD_UI_MODE";

const ENV_VALUE_TD_UI_MODE_INTERNAL: &str = "internal";
const ENV_VALUE_TD_UI_MODE_EXTERNAL: &str = "external";

const ENV_TD_UI_DIR: &str = "TD_UI_DIR";
const ENV_TD_UI_INDEX: &str = "TD_UI_INDEX";

const PARENT_FOLDER: &str = "..";

const TABSDATA_UI_PROJECT_FOLDER: &str = "tabsdata-ui";
const TARGET_FOLDER: &str = "target";

const INDEX_HTML_FILE: &str = "index.html";

pub fn set_environment_variables() -> Result<(), Box<dyn Error>> {
    let ui_mode =
        env::var(ENV_TD_UI_MODE).unwrap_or_else(|_| ENV_VALUE_TD_UI_MODE_EXTERNAL.to_string());
    println!("cargo:rustc-env=TD_UI_MODE={ui_mode}");
    let root = find_workspace_root();
    let ui_dir_path = root
        .join(PARENT_FOLDER)
        .join(TABSDATA_UI_PROJECT_FOLDER)
        .join(TARGET_FOLDER);
    fs::create_dir_all(&ui_dir_path)?;
    let ui_dir = ui_dir_path
        .to_str()
        .ok_or("Failed to convert ui_dir_path to string")?;
    let ui_index = if ui_mode == ENV_VALUE_TD_UI_MODE_INTERNAL {
        INDEX_HTML_FILE.to_owned()
    } else {
        let ui_index_path = ui_dir_path.join(INDEX_HTML_FILE);
        ui_index_path
            .to_str()
            .ok_or("Failed to convert ui_index_path to string")?
            .to_owned()
    };
    println!("cargo:rustc-env={ENV_TD_UI_DIR}={ui_dir}");
    println!("cargo:rustc-env={ENV_TD_UI_INDEX}={ui_index}");

    Ok(())
}
