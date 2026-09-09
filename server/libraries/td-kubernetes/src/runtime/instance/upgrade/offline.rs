//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

use serde::Serialize;

use crate::core::system::exit::CommandResult;
use crate::runtime::instance::profile::bundle::parse_filename;
use crate::runtime::instance::upgrade::apply::apply_step;
use crate::runtime::instance::upgrade::catalog::load_spec;
use crate::runtime::instance::upgrade::plan::Step;
use crate::runtime::instance::upgrade::spec::{Action, Spec};
use crate::runtime::instance::upgrade::store::{FolderStore, family_of};

#[derive(Debug, Serialize)]
pub struct Upgraded {
    pub version: String,
    pub steps: Vec<Outcome>,
}

#[derive(Debug, Serialize)]
pub struct Outcome {
    pub family: String,
    pub file: String,
    pub variant: String,
    pub action: String,
    pub changed: bool,
}

fn bundle_variants(kprofile_folder: &Path, folder: &str, file: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(kprofile_folder.join(folder)) else {
        return Vec::new();
    };
    let mut variants: Vec<String> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| parse_filename(name).render_stripped() == file)
        .collect();
    variants.sort();
    variants
}

fn steps_of(spec: &Spec, kprofile_folder: &Path) -> CommandResult<Vec<Step>> {
    let mut steps = Vec::new();
    for (family, groups) in spec.families() {
        let folder = family_of(family)?.folder;
        for (file, group) in groups {
            for (variant, action) in group {
                let expandable = matches!(action, Action::Modify { .. } | Action::Delete(_));
                let targets = if !expandable || kprofile_folder.join(folder).join(variant).exists()
                {
                    vec![variant.clone()]
                } else {
                    bundle_variants(kprofile_folder, folder, file)
                };
                for target in targets {
                    steps.push(Step {
                        entry: format!("{folder}/{file}"),
                        folder,
                        file: file.clone(),
                        variant: target,
                        action: action.clone(),
                    });
                }
            }
        }
    }
    Ok(steps)
}

pub async fn upgrade_kprofile(
    kupgrade_folder: &Path,
    kprofile_folder: &Path,
    version: &str,
) -> CommandResult<Upgraded> {
    let spec = load_spec(kupgrade_folder, version)?;
    let store = FolderStore::new(kprofile_folder);
    let mut outcomes = Vec::new();

    for step in steps_of(&spec, kprofile_folder)? {
        let applied = apply_step(&store, kupgrade_folder, version, &step).await?;
        outcomes.push(Outcome {
            family: step.folder.to_owned(),
            file: step.file.clone(),
            variant: step.variant.clone(),
            action: step.action.as_ref().to_owned(),
            changed: applied.changed(),
        });
    }

    Ok(Upgraded {
        version: version.to_owned(),
        steps: outcomes,
    })
}
