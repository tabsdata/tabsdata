//
// Copyright 2025 Tabsdata Inc.
//

use crate::structure::find_workspace_root;
use std::error::Error;
use std::path::Path;
use std::{env, fs};

const ENV_TD_UI_MODE: &str = "TD_UI_MODE";

pub const ENV_VALUE_TD_UI_MODE_INTERNAL: &str = "internal";
pub const ENV_VALUE_TD_UI_MODE_EXTERNAL: &str = "external";

const ENV_TD_UI_DIR: &str = "TD_UI_DIR";
const ENV_TD_UI_INDEX: &str = "TD_UI_INDEX";

const ENV_TD_NEW_UI: &str = "TD_NEW_UI";
const CFG_TD_NEW_UI: &str = "td_new_ui";

const PARENT_FOLDER: &str = "..";

const TABSDATA_UI_PROJECT_FOLDER: &str = "tabsdata-ui";
const TARGET_FOLDER: &str = "target";
const NEW_UI_FOLDER: &str = "new-ui";

const INDEX_HTML_FILE: &str = "index.html";

pub fn set_environment_variables() -> Result<(), Box<dyn Error>> {
    match env::var(ENV_TD_UI_MODE) {
        Ok(value) => eprintln!("✅ ENV_TD_UI_MODE is set: '{value}'"),
        Err(env::VarError::NotPresent) => eprintln!("⚠️ TD_UI_MODE is not set"),
        Err(err) => eprintln!("❌ Error accessing ENV_TD_UI_MODE: {err}"),
    }

    let ui_mode =
        env::var(ENV_TD_UI_MODE).unwrap_or_else(|_| ENV_VALUE_TD_UI_MODE_EXTERNAL.to_string());
    println!("cargo:rustc-env=TD_UI_MODE={ui_mode}");
    let ui_dir_path = find_workspace_root()
        .join(PARENT_FOLDER)
        .join(TABSDATA_UI_PROJECT_FOLDER)
        .join(TARGET_FOLDER);
    fs::create_dir_all(&ui_dir_path)?;
    let ui_dir = ui_dir_path
        .to_str()
        .ok_or("Failed to convert ui_dir_path to string")?;
    let ui_index = match ui_mode.as_str() {
        ENV_VALUE_TD_UI_MODE_INTERNAL => INDEX_HTML_FILE.to_owned(),
        ENV_VALUE_TD_UI_MODE_EXTERNAL => {
            let ui_index_path = ui_dir_path.join(INDEX_HTML_FILE);
            ui_index_path
                .to_str()
                .ok_or("Failed to convert ui_index_path to string")?
                .to_owned()
        }
        other => return Err(format!("Unsupported TD_UI_MODE: {other}").into()),
    };

    println!("cargo:rustc-env={ENV_TD_UI_DIR}={ui_dir}");
    println!("cargo:rustc-env={ENV_TD_UI_INDEX}={ui_index}");

    println!("cargo::rustc-check-cfg=cfg({CFG_TD_NEW_UI})");
    if env::var(ENV_TD_NEW_UI).is_ok_and(|value| value == "true") {
        let new_ui_index_path = ui_dir_path.join(NEW_UI_FOLDER).join(INDEX_HTML_FILE);
        if !new_ui_index_path.is_file() {
            return Err(format!(
                "{ENV_TD_NEW_UI} is 'true' but the new UI is not built: missing '{}'",
                new_ui_index_path.display()
            )
            .into());
        }
        println!("cargo::rustc-cfg={CFG_TD_NEW_UI}");
    }

    println!("cargo:rerun-if-env-changed={ENV_TD_UI_MODE}");
    println!("cargo:rerun-if-env-changed={ENV_TD_NEW_UI}");
    println!("cargo:rerun-if-changed={ui_dir}");
    for entry in walk_ui_folder(&ui_dir_path) {
        println!("cargo:rerun-if-changed={entry}");
    }

    Ok(())
}

fn walk_ui_folder(folder: &Path) -> Vec<String> {
    let mut entries = Vec::new();
    let mut pending = vec![folder.to_path_buf()];
    while let Some(current) = pending.pop() {
        let Ok(children) = fs::read_dir(&current) else {
            continue;
        };
        for child in children.flatten() {
            let path = child.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with('.'))
            {
                continue;
            }
            if path.is_dir() {
                pending.push(path);
            } else if let Some(name) = path.to_str() {
                entries.push(name.to_owned());
            }
        }
    }
    entries
}
