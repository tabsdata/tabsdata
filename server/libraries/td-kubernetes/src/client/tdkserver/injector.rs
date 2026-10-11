//
// Copyright 2026 Tabsdata Inc.
//

mod engine;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::str::FromStr;

use serde_yaml::Value;

use crate::base::constants::KPROFILE_CONFIGS_FAMILY;
use crate::base::error::cli::CliError;
use crate::base::error::injector::InjectorError;
use crate::client::tdkserver::injector::engine::address::Segment;
use crate::client::tdkserver::injector::engine::document::{
    Document, Edit, Leaf, Lookup, apply, set_at,
};
use crate::client::tdkserver::injector::engine::value::{
    LeafType, placeholder_text, render_leaf, render_placeholder,
};
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    Placeholder, Rule, contains_placeholder, parse_bare_placeholder, placeholder_occurrences,
};
use crate::runtime::instance::profile::RenderedConfigmap;
use crate::runtime::instance::tabsdata::TABSDATA_FILE;

pub use engine::address::{Address, CONFIG_EXTENSION};
pub use engine::source::{CONFIG_ARGUMENT, FILE_PREFIX, Setting, key_address, parse_settings};
pub use engine::value::Provided;

pub const CONFIG_HELP: &str = "Sets a value of the instance configuration, as ADDRESS=VALUE \
or as @FILE with several settings. Repeat it to set several values. Run with '--help' for \
the address forms, the settings file syntax and examples.";

pub const CONFIG_LONG_HELP: &str = "\
Sets a value of the instance configuration. The value is written into the \
config files under 'init/input/configs' of the new instance before anything \
reads them. Repeat the option to set several values. Each value can be set only \
once, even with the same content. Every setting is checked before the instance \
folder is created, so an invalid setting leaves nothing behind.

A setting has the form ADDRESS=VALUE, where ADDRESS names what to set in one of \
three ways.

By placeholder category:
  Config files hold placeholders such as '${{literal:tdk_database_host}}' or \
'${{secret:postgres_system_password}}'. The word before the colon is the \
placeholder kind (literal, literalAuto, secret or secretAuto can be set), and \
the word after it is the category. Naming the category sets every placeholder \
of that category, in every config file at once:
    tdk_database_host=db.internal
  The kind can be prefixed to the category, and it must be when two kinds \
share one category:
    secret:postgres_system_user=tabsdata

By file and key path:
  Any final value of a config file, placeholder or not, is addressed by the \
file name (with or without '.yaml'), a colon, and the path of keys down to the \
value, joined by dots:
    tabsdata:pods.service.resources.cpu=4
  A list item is selected by its position, starting at 0, or by the value of \
one of its keys:
    tabsdata:pods.service.placement.tolerations[0].effect=NoExecute
    tabsdata:pods.service.placement.tolerations[operator=Exists].effect=NoExecute
  A key that contains dots is quoted, as in 'section.\"key.with.dots\".value'.

From a settings file:
  '@FILE' reads settings from a YAML file. It may be a list of 'ADDRESS=VALUE' \
strings, a mapping of addresses to values, or a mapping with two sections: \
'files', with the keys of each config file nested as in the file itself, and \
'values', a mapping of addresses to values:
    files:
      tabsdata:
        pods:
          service:
            resources:
              cpu: \"4\"
    values:
      tdk_database_host: db.internal
      secret:postgres_system_password: env:POSTGRES_PASSWORD

Only values that already exist can be set: never new keys, and never a key \
that holds other keys or a list. The one exception is 'tabsdata:pods.global.envs', \
where new environment variables can be added for every pod and function:
    tabsdata:pods.global.envs.MY_API_KEY=env:MY_API_KEY
Their values are written as strings. A string value replaces a string, while numbers \
and booleans replace only values of their own type. A VALUE starting with \
'env:' is read from that environment variable of this machine, so secrets need \
not be written in any file of yours. To combine variables with fixed text, wrap \
each variable in '${...}': 'env:${BUCKET_URI}/root' reads BUCKET_URI and appends \
'/root'. 'val:' keeps a value that literally starts with 'env:'. Secret values \
end up in the instance input config files.

Examples:
  tdkserver init --provider gke --config tdk_database_host=db.internal \
--config secret:postgres_system_password=env:POSTGRES_PASSWORD
  tdkserver quickstart --config tabsdata:pods.service.resources.memory=8Gi
  tdkserver quickstart --config anthropic-key=env:ANTHROPIC_API_KEY
  tdkserver quickstart --config tabsdata:pods.global.envs.MY_API_KEY=env:MY_API_KEY
  tdkserver quickstart --config @settings.yaml";

const MAX_SUGGESTION_DISTANCE: usize = 3;

const GLOBAL_ENVS_PATH: [&str; 3] = ["pods", "global", "envs"];

struct Occurrence<'a> {
    file: &'a str,
    leaf: Leaf<'a>,
    placeholder: Placeholder,
}

struct Target {
    file: String,
    edit: Edit,
}

pub fn plan(
    configs: &RenderedConfigmap,
    settings: &[Setting],
) -> Result<BTreeMap<String, String>, InjectorError> {
    if settings.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut documents: BTreeMap<&str, Document> = BTreeMap::new();
    for (file, content) in configs {
        if !file.ends_with(CONFIG_EXTENSION) {
            continue;
        }
        let document =
            Document::parse(content).map_err(|cause| InjectorError::ConfigParseFailed {
                file: file.clone(),
                cause,
            })?;
        documents.insert(file.as_str(), document);
    }

    let mut bare: Vec<Occurrence> = Vec::new();
    let mut embedded: Vec<Placeholder> = Vec::new();
    for (file, document) in &documents {
        for leaf in document.leaves() {
            if !contains_placeholder(&leaf.scalar.value) {
                continue;
            }
            match parse_bare_placeholder(&leaf.scalar.value) {
                Some(placeholder) => bare.push(Occurrence {
                    file,
                    leaf,
                    placeholder,
                }),
                None => embedded.extend(placeholder_occurrences(&leaf.scalar.value)),
            }
        }
    }

    let mut claimed: HashMap<String, String> = HashMap::new();
    let mut targets: Vec<Target> = Vec::new();
    for setting in settings {
        let (identity, shown, resolved) = match &setting.address {
            Address::Category { kind, category } => {
                category_targets(setting, *kind, category, &bare, &embedded, &documents)?
            }
            Address::Key { file, path } => key_target(setting, file, path, &documents)?,
        };
        if let Some(first) = claimed.get(&identity) {
            return Err(InjectorError::DuplicateSetting {
                first: first.clone(),
                second: setting.origin.clone(),
                target: shown,
            });
        }
        claimed.insert(identity, setting.origin.clone());
        targets.extend(resolved);
    }

    let mut by_file: BTreeMap<String, Vec<Edit>> = BTreeMap::new();
    for target in targets {
        by_file.entry(target.file).or_default().push(target.edit);
    }
    let mut injected = BTreeMap::new();
    for (file, edits) in by_file {
        let document = &documents[file.as_str()];
        let content = apply(document.source(), &edits);
        verify(&file, document.source(), &content, &edits)?;
        injected.insert(file, content);
    }
    Ok(injected)
}

pub fn write(input: &Path, injected: &BTreeMap<String, String>) -> Result<(), CliError> {
    let folder = input.join(KPROFILE_CONFIGS_FAMILY.folder);
    for (file, content) in injected {
        let target = folder.join(file);
        std::fs::write(&target, content).map_err(|cause| CliError::FileWriteFailed {
            file: target.display().to_string(),
            cause: cause.to_string(),
        })?;
    }
    Ok(())
}

fn category_targets(
    setting: &Setting,
    kind: Option<PlaceholderKind>,
    category: &str,
    bare: &[Occurrence],
    embedded: &[Placeholder],
    documents: &BTreeMap<&str, Document>,
) -> Result<(String, String, Vec<Target>), InjectorError> {
    let origin = &setting.origin;
    let named = |placeholder: &Placeholder| placeholder.category.as_deref() == Some(category);
    let kinds: BTreeSet<&str> = bare
        .iter()
        .map(|occurrence| &occurrence.placeholder)
        .chain(embedded)
        .filter(|placeholder| named(placeholder))
        .map(|placeholder| placeholder.kind.as_str())
        .collect();
    let chosen = match kind {
        Some(kind) if kinds.contains(kind.as_str()) => kind.as_str(),
        Some(_) | None if kinds.is_empty() => {
            return Err(InjectorError::UnknownCategory {
                origin: origin.clone(),
                category: category.to_owned(),
                suggestion: suggestion(category, bare),
            });
        }
        Some(_) => {
            return Err(InjectorError::UnknownCategory {
                origin: origin.clone(),
                category: setting.address.to_string(),
                suggestion: format!(" It is used as: {}.", qualified(&kinds, category)),
            });
        }
        None if kinds.len() > 1 => {
            return Err(InjectorError::AmbiguousCategory {
                origin: origin.clone(),
                category: category.to_owned(),
                count: kinds.len(),
                candidates: qualified(&kinds, category),
            });
        }
        None => kinds.iter().next().copied().unwrap_or_default(),
    };
    let identity = format!("{chosen}:{category}");
    let resolved = editable_kind(chosen, &identity, origin)?;

    let occurrences: Vec<&Occurrence> = bare
        .iter()
        .filter(|occurrence| {
            named(&occurrence.placeholder) && occurrence.placeholder.kind == chosen
        })
        .collect();
    let text = placeholder_text(&setting.value).map_err(|_| InjectorError::ValueNotScalar {
        origin: origin.clone(),
    })?;
    let mandatory = occurrences
        .iter()
        .any(|occurrence| occurrence.placeholder.rule == Rule::Mandatory);
    if text.is_empty() && mandatory && !resolved.is_auto() {
        return Err(InjectorError::EmptyMandatoryValue {
            origin: origin.clone(),
            identity,
        });
    }
    let mut targets = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let rendered = render_placeholder(&text, occurrence.leaf.scalar.flow);
        let edit = documents[occurrence.file]
            .edit(&occurrence.leaf, &rendered)
            .map_err(|reason| InjectorError::UnsupportedLayout {
                origin: origin.clone(),
                address: identity.clone(),
                reason,
            })?;
        targets.push(Target {
            file: occurrence.file.to_owned(),
            edit,
        });
    }
    Ok((format!("placeholder {identity}"), identity, targets))
}

fn key_target(
    setting: &Setting,
    file: &str,
    path: &[Segment],
    documents: &BTreeMap<&str, Document>,
) -> Result<(String, String, Vec<Target>), InjectorError> {
    let origin = &setting.origin;
    let address = setting.address.to_string();
    let Some(document) = documents.get(file) else {
        return Err(InjectorError::UnknownFile {
            origin: origin.clone(),
            file: file.to_owned(),
            available: documents.keys().copied().collect::<Vec<_>>().join(", "),
        });
    };
    let found = document.find(path);
    if matches!(found, Err(Lookup::Missing))
        && let Some(name) = global_env_name(file, path)
    {
        return global_env_target(setting, file, path, name, document);
    }
    let leaf = found.map_err(|lookup| match lookup {
        Lookup::Missing => InjectorError::KeyNotFound {
            origin: origin.clone(),
            address: address.clone(),
        },
        Lookup::NotALeaf => InjectorError::NotALeaf {
            origin: origin.clone(),
            address: address.clone(),
        },
        Lookup::Alias => InjectorError::UnsupportedLayout {
            origin: origin.clone(),
            address: address.clone(),
            reason: "it is an alias".to_owned(),
        },
        Lookup::Selector { selector, count } => InjectorError::SelectorMismatch {
            origin: origin.clone(),
            address: address.clone(),
            selector,
            count,
        },
    })?;

    let rendered = if contains_placeholder(&leaf.scalar.value) {
        let Some(placeholder) = parse_bare_placeholder(&leaf.scalar.value) else {
            return Err(InjectorError::NonEditableKey {
                origin: origin.clone(),
                address,
            });
        };
        let kind = PlaceholderKind::from_str(&placeholder.kind)
            .ok()
            .filter(|kind| kind.is_editable() && kind.is_supported());
        let Some(kind) = kind else {
            return Err(InjectorError::NonEditableKey {
                origin: origin.clone(),
                address,
            });
        };
        if let Some(category) = placeholder.category.as_deref().filter(|c| !c.is_empty()) {
            return Err(InjectorError::PlaceholderAtKey {
                origin: origin.clone(),
                address,
                identity: format!("{}:{category}", kind.as_str()),
            });
        }
        let text = placeholder_text(&setting.value).map_err(|_| InjectorError::ValueNotScalar {
            origin: origin.clone(),
        })?;
        if text.is_empty() && placeholder.rule == Rule::Mandatory && !kind.is_auto() {
            return Err(InjectorError::EmptyMandatoryValue {
                origin: origin.clone(),
                identity: address,
            });
        }
        render_placeholder(&text, leaf.scalar.flow)
    } else {
        let leaf_type = LeafType::of(leaf.scalar);
        render_leaf(&setting.value, leaf_type, leaf.scalar.flow).map_err(|reason| {
            InjectorError::ValueTypeMismatch {
                origin: origin.clone(),
                address: address.clone(),
                expected: leaf_type.describe().to_owned(),
                reason,
            }
        })?
    };
    let edit =
        document
            .edit(&leaf, &rendered)
            .map_err(|reason| InjectorError::UnsupportedLayout {
                origin: origin.clone(),
                address: address.clone(),
                reason,
            })?;
    let identity = format!("key {file} {:?}", edit.steps);
    Ok((
        identity,
        address,
        vec![Target {
            file: file.to_owned(),
            edit,
        }],
    ))
}

fn global_env_name<'a>(file: &str, path: &'a [Segment]) -> Option<&'a str> {
    let [parent @ .., name] = path else {
        return None;
    };
    let expandable = file == TABSDATA_FILE
        && path.iter().all(|segment| segment.selectors.is_empty())
        && parent
            .iter()
            .map(|segment| segment.key.as_str())
            .eq(GLOBAL_ENVS_PATH);
    expandable.then_some(name.key.as_str())
}

fn global_env_target(
    setting: &Setting,
    file: &str,
    path: &[Segment],
    name: &str,
    document: &Document,
) -> Result<(String, String, Vec<Target>), InjectorError> {
    let origin = &setting.origin;
    let address = setting.address.to_string();
    if !is_env_name(name) {
        return Err(InjectorError::InvalidEnvName {
            origin: origin.clone(),
            address,
            name: name.to_owned(),
        });
    }
    let rendered = render_leaf(&setting.value, LeafType::Text, false).map_err(|reason| {
        InjectorError::ValueTypeMismatch {
            origin: origin.clone(),
            address: address.clone(),
            expected: LeafType::Text.describe().to_owned(),
            reason,
        }
    })?;
    let parent = &path[..path.len() - 1];
    let edit = document.insert(parent, name, &rendered).map_err(|reason| {
        InjectorError::UnsupportedLayout {
            origin: origin.clone(),
            address: address.clone(),
            reason,
        }
    })?;
    let identity = format!("key {file} {:?}", edit.steps);
    Ok((
        identity,
        address,
        vec![Target {
            file: file.to_owned(),
            edit,
        }],
    ))
}

fn is_env_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn editable_kind(
    kind: &str,
    identity: &str,
    origin: &str,
) -> Result<PlaceholderKind, InjectorError> {
    let Ok(resolved) = PlaceholderKind::from_str(kind) else {
        return Err(InjectorError::UnsupportedPlaceholder {
            origin: origin.to_owned(),
            identity: identity.to_owned(),
        });
    };
    if !resolved.is_editable() {
        return Err(InjectorError::NonEditablePlaceholder {
            origin: origin.to_owned(),
            identity: identity.to_owned(),
            kind: kind.to_owned(),
        });
    }
    if !resolved.is_supported() {
        return Err(InjectorError::UnsupportedPlaceholder {
            origin: origin.to_owned(),
            identity: identity.to_owned(),
        });
    }
    Ok(resolved)
}

fn qualified(kinds: &BTreeSet<&str>, category: &str) -> String {
    kinds
        .iter()
        .map(|kind| format!("'{kind}:{category}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn suggestion(category: &str, bare: &[Occurrence]) -> String {
    let editable: BTreeSet<&str> = bare
        .iter()
        .filter(|occurrence| {
            PlaceholderKind::from_str(&occurrence.placeholder.kind)
                .is_ok_and(|kind| kind.is_editable() && kind.is_supported())
        })
        .filter_map(|occurrence| occurrence.placeholder.category.as_deref())
        .filter(|candidate| !candidate.is_empty())
        .collect();
    editable
        .iter()
        .map(|candidate| (distance(category, candidate), *candidate))
        .filter(|(distance, _)| *distance <= MAX_SUGGESTION_DISTANCE)
        .min()
        .map(|(_, candidate)| format!(" Did you mean '{candidate}'?"))
        .unwrap_or_default()
}

fn distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    for (row, left_char) in left.chars().enumerate() {
        let mut current = vec![row + 1];
        for (column, right_char) in right.iter().enumerate() {
            let substitution = previous[column] + usize::from(left_char != *right_char);
            current.push(
                substitution
                    .min(previous[column + 1] + 1)
                    .min(current[column] + 1),
            );
        }
        previous = current;
    }
    previous[right.len()]
}

fn verify(file: &str, original: &str, injected: &str, edits: &[Edit]) -> Result<(), InjectorError> {
    let failed = |cause: String| InjectorError::InjectionVerifyFailed {
        file: file.to_owned(),
        cause,
    };
    let mut expected: Value =
        serde_yaml::from_str(original).map_err(|cause| failed(cause.to_string()))?;
    let actual: Value =
        serde_yaml::from_str(injected).map_err(|cause| failed(cause.to_string()))?;
    for edit in edits {
        let replacement: Value =
            serde_yaml::from_str(edit.value.trim()).map_err(|cause| failed(cause.to_string()))?;
        if !set_at(&mut expected, &edit.steps, replacement) {
            return Err(failed("an edited value could not be located".to_owned()));
        }
    }
    if expected != actual {
        return Err(failed(
            "the edited file differs from the original beyond the edited values".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
