//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use crate::base::constants::{
    Family, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
    SCHEMA_SOURCE_EXTENSION,
};
use crate::runtime::instance::profile::bundle::{
    Axis, ParsedFileName, parse_filename, pick_winner,
};
use crate::runtime::instance::upgrade::spec::{Action, Group, Spec};

#[derive(Clone, Debug, PartialEq)]
pub struct Step {
    pub entry: String,
    pub folder: &'static str,
    pub file: String,
    pub variant: String,
    pub action: Action,
}

pub fn plan(spec: &Spec, axes: &[Axis], preferred: &[&'static str]) -> Vec<Step> {
    let mut configs = spec.configs.clone();
    configs.extend(spec.resolved.configs.clone());
    let families: [(&'static Family, &BTreeMap<String, Group>); 3] = [
        (&KPROFILE_ASSETS_FAMILY, &spec.assets),
        (&KPROFILE_CONFIGS_FAMILY, &configs),
        (&KPROFILE_PODS_FAMILY, &spec.pods),
    ];
    let mut steps = Vec::new();
    for (family, groups) in families {
        for (file, group) in groups {
            if family.folder == KPROFILE_CONFIGS_FAMILY.folder
                && file.ends_with(SCHEMA_SOURCE_EXTENSION)
            {
                continue;
            }
            let names: Vec<&String> = group.keys().collect();
            let candidates: Vec<ParsedFileName> = names
                .iter()
                .map(|variant| parse_filename(variant))
                .collect();
            let Some(winner) = pick_winner(&candidates, axes, preferred) else {
                continue;
            };
            let name = names[winner].clone();
            steps.push(Step {
                entry: format!("{}/{file}", family.folder),
                folder: family.folder,
                file: file.clone(),
                action: group[&name].clone(),
                variant: name,
            });
        }
    }
    steps
}

#[cfg(test)]
mod tests;
