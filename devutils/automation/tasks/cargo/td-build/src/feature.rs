//
// Copyright 2024 Tabs Data Inc.
//

use crate::descriptor::MANIFEST_FOLDER;
use crate::structure::find_workspace_root;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::read_to_string;
use std::path::Path;
use toml::Value;

const ENV_CARGO_MANIFEST_DIR: &str = "CARGO_MANIFEST_DIR";
const ENV_CARGO_MANIFEST_PATH: &str = "CARGO_MANIFEST_PATH";

const TAG_WORKSPACE: &str = "workspace";
const TAG_MEMBERS: &str = "members";
const TAG_FEATURES: &str = "features";

const CARGO_FILE: &str = "Cargo.toml";
const MANIFEST_FILE: &str = "Manifest.toml";

const FEATURE_FILE: &str = "feature.yaml";

const ENV_TD_DISABLE_OPENAPI_DOCS_ENDPOINT: &str = "TD_DISABLE_OPENAPI_DOCS_ENDPOINT";
const ENABLE_API_DOCS_FEATURE: &str = "api-docs";

#[derive(Debug)]
struct Feature {
    features: Vec<String>,
}

impl<'de> Deserialize<'de> for Feature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let features = Vec::<String>::deserialize(deserializer).unwrap_or_default();
        Ok(Feature { features })
    }
}

pub fn set_cargo_features() -> Result<(), Box<dyn Error>> {
    let features_file = find_workspace_root()
        .join(MANIFEST_FOLDER)
        .join(FEATURE_FILE);
    if features_file.exists() {
        let features_content = read_to_string(features_file)?;
        let features: Feature =
            serde_yaml::from_str(&features_content).unwrap_or(Feature { features: vec![] });
        for feature in features.features {
            let configuration = format!("cargo:rustc-cfg=feature=\"{feature}\"");
            println!("{configuration}");
        }
    }

    let disable_openapi = env::var(ENV_TD_DISABLE_OPENAPI_DOCS_ENDPOINT).unwrap_or_default();
    // Enable /api/docs unless explicitly disabled.
    if disable_openapi != "true" {
        println!("cargo:rustc-cfg=feature=\"{ENABLE_API_DOCS_FEATURE}\"");
    }

    Ok(())
}

pub fn check_cargo_features() -> Result<(), Box<dyn Error>> {
    let workspace_cargo_file = env::var(ENV_CARGO_MANIFEST_PATH)
        .or_else(|_| {
            env::var(ENV_CARGO_MANIFEST_DIR).map(|dir| {
                Path::new(&dir)
                    .join(CARGO_FILE)
                    .to_str()
                    .expect("⛔️ Failed to build workspace Cargo.toml file")
                    .to_string()
            })
        })
        .unwrap_or_else(|_| {
            panic!(
                "⛔️ Failed to get workspace Cargo.toml file. Both '{ENV_CARGO_MANIFEST_PATH}' and '{ENV_CARGO_MANIFEST_DIR}' are unset."
            )
        });

    let workspace_cargo =
        read_to_string(&workspace_cargo_file).expect("⛔️ Failed to read workspace Cargo.toml file");
    let workspace_toml: Value =
        toml::from_str(&workspace_cargo).expect("⛔️ Failed to parse workspace Cargo.toml file");

    let workspace_root = Path::new(&workspace_cargo_file)
        .parent()
        .expect("⛔️ Failed to get workspace root");

    let workspace_descriptor_file = workspace_root.join(MANIFEST_FILE);
    let workspace_descriptor = read_to_string(&workspace_descriptor_file)
        .expect("⛔️ Failed to read workspace Manifest.toml file");
    let workspace_descriptor_content: Value =
        toml::from_str(&workspace_descriptor).expect("⛔️ Failed to parse workspace Manifest.toml");
    let descriptor_features = workspace_descriptor_content
        .get(TAG_FEATURES)
        .and_then(|f| f.as_table())
        .ok_or(format!("⛔️ No [{TAG_FEATURES}] section in Manifest.toml"))
        .expect("⛔️ Failed to extract metadata from Manifest.toml");
    let descriptor_features_map: HashMap<String, Vec<String>> = descriptor_features
        .iter()
        .map(|(key, value)| {
            let subfeatures = value
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            (key.clone(), subfeatures)
        })
        .collect();

    let workspace_members = workspace_toml
        .get(TAG_WORKSPACE)
        .and_then(|w| w.get(TAG_MEMBERS))
        .and_then(|m| m.as_array())
        .expect("⛔️ No workspace members in Cargo.toml")
        .iter()
        .filter_map(|m| m.as_str())
        .map(|m| workspace_root.join(m))
        .chain(Some(workspace_root.to_path_buf()))
        .collect::<Vec<_>>();

    let mut missing_features_report = Vec::new();
    for workspace_member in workspace_members {
        let member_cargo_file = workspace_member.join(CARGO_FILE);
        let member_cargo = read_to_string(&member_cargo_file).unwrap_or_else(|_| {
            panic!(
                "⛔️ Failed to read member Cargo.toml file: '{}'",
                member_cargo_file.display()
            )
        });
        let member_toml: Value = toml::from_str(&member_cargo).unwrap_or_else(|_| {
            panic!(
                "⛔️ Failed to parse member Cargo.toml file: '{}'",
                member_cargo_file.display()
            )
        });
        let member_features = member_toml
            .get(TAG_FEATURES)
            .and_then(|f| f.as_table())
            .ok_or(format!(
                "⛔️ No [{TAG_FEATURES}] section in {member_cargo_file:?}"
            ))
            .expect("⛔️ Failed to extract metadata from member Cargo.toml");

        for (descriptor_feature, descriptor_subfeatures) in &descriptor_features_map {
            if !member_features.contains_key(descriptor_feature) {
                missing_features_report.push(format!(
                    "⛔️ Crate '{workspace_member:?}' is missing feature '{descriptor_feature}'"
                ));
                continue;
            }

            let member_subfeatures: Vec<_> = member_features
                .get(descriptor_feature)
                .and_then(|f| f.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            if member_subfeatures != *descriptor_subfeatures {
                missing_features_report.push(format!(
                    "⛔️ Crate '{workspace_member:?}': Feature '{descriptor_feature}' has mismatched subfeatures. Expected: {descriptor_subfeatures:?}, Found: {member_subfeatures:?}"
                ));
            }
        }
    }
    if !missing_features_report.is_empty() {
        eprintln!(
            "⛔️ Some crates are missing declared required (test) features:\n{}",
            missing_features_report.join("\n")
        );
        std::process::exit(1);
    }
    Ok(())
}
