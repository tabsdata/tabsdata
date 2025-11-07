//
// Copyright 2025 Tabs Data Inc.
//

use serde::{Deserialize, Deserializer};
use std::fs::read_to_string;
use std::path::PathBuf;
use tm_workspace::workspace_root;

const WORKSPACE_ROOT: &str = workspace_root!();

const MANIFEST_FOLDER: &str = ".manifest";

const FEATURE_FILE: &str = "feature.yaml";

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

pub fn boot() {
    features()
}

fn features() {
    let features_file = PathBuf::from(WORKSPACE_ROOT)
        .join(MANIFEST_FOLDER)
        .join(FEATURE_FILE);
    if features_file.exists() {
        let features_content = read_to_string(features_file).unwrap();
        let features: Feature =
            serde_yaml::from_str(&features_content).unwrap_or(Feature { features: vec![] });
        for feature in features.features {
            let configuration = format!("cargo:rustc-cfg=feature=\"{feature}\"");
            println!("{configuration}");
        }
    }
}
