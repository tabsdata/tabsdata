//
// Copyright 2026 Tabsdata Inc.
//

use serde_yaml::{Mapping, Value};
use td_database::components::{resolved_schema_of, resolved_username_of};
use td_database::config::DatabaseComponents;

use crate::base::error::gateway::ResolverError;

const PATH_SEPARATOR: &str = ".";
const APISERVER_DATABASE_CONFIG: &str = "apiserver.database.yaml";
const DATABASE_SECTION: &str = "database";
const BACKEND_PROPERTY: &str = "postgres";
const DATABASE_PROPERTY: &str = "database";
const DEFAULT_SCHEMA_PREFIX: &str = "td_";
const SCHEMA_PROPERTY: &str = "schema";
const USERNAME_PROPERTY: &str = "username";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedRule {
    ComponentSchema,
    ComponentUsername,
    #[allow(dead_code)]
    SameAs {
        property: &'static str,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct DerivedProperty {
    pub section: &'static str,
    pub container: Option<&'static str>,
    pub property: &'static str,
    pub after: &'static str,
    pub rule: DerivedRule,
}

pub const DERIVED_PROPERTIES: &[(&str, &[DerivedProperty])] = &[(
    APISERVER_DATABASE_CONFIG,
    &[
        DerivedProperty {
            section: DATABASE_SECTION,
            container: None,
            property: SCHEMA_PROPERTY,
            after: BACKEND_PROPERTY,
            rule: DerivedRule::ComponentSchema,
        },
        DerivedProperty {
            section: DATABASE_SECTION,
            container: Some(BACKEND_PROPERTY),
            property: USERNAME_PROPERTY,
            after: DATABASE_PROPERTY,
            rule: DerivedRule::ComponentUsername,
        },
    ],
)];

pub fn derived_properties(file: &str) -> &'static [DerivedProperty] {
    DERIVED_PROPERTIES
        .iter()
        .find(|(name, _)| *name == file)
        .map(|(_, properties)| *properties)
        .unwrap_or_default()
}

pub fn amend(file: &str, rendered: &str) -> Result<String, ResolverError> {
    let properties = derived_properties(file);
    if properties.is_empty() {
        return Ok(rendered.to_owned());
    }
    let mut value: Value =
        serde_yaml::from_str(rendered).map_err(|cause| ResolverError::ConfigParseFailed {
            cause: cause.to_string(),
        })?;
    if value.is_null() {
        return Ok(rendered.to_owned());
    }

    for property in properties {
        for component in components_in(&value, property.section) {
            if let Some(text) = resolve(property, &value, &component) {
                insert(&mut value, property, &component, &text);
            }
        }
    }

    serde_yaml::to_string(&value).map_err(|cause| ResolverError::ConfigSerializeFailed {
        cause: cause.to_string(),
    })
}

fn components_in(value: &Value, section: &str) -> Vec<String> {
    let Some(Value::Mapping(map)) = node_at(value, section) else {
        return Vec::new();
    };
    map.keys()
        .filter_map(|key| key.as_str().map(str::to_owned))
        .collect()
}

fn resolve(property: &DerivedProperty, value: &Value, component: &str) -> Option<String> {
    let anchor = node_at(value, &anchor_of(property.section, component))?;
    let holder = holder(anchor, property)?;
    if holder.get(property.property).is_some() {
        return None;
    }
    let components: DatabaseComponents =
        serde_yaml::from_value(node_at(value, property.section)?.clone()).ok()?;
    let default = default_schema(component);
    match property.rule {
        DerivedRule::ComponentSchema => resolved_schema_of(&components, component, &default).ok(),
        DerivedRule::ComponentUsername => {
            resolved_username_of(&components, component, &default).ok()
        }
        DerivedRule::SameAs { .. } => None,
    }
}

fn default_schema(component: &str) -> String {
    format!("{DEFAULT_SCHEMA_PREFIX}{component}")
}

fn anchor_of(section: &str, component: &str) -> String {
    format!("{section}{PATH_SEPARATOR}{component}")
}

fn holder<'a>(anchor: &'a Value, property: &DerivedProperty) -> Option<&'a Value> {
    match property.container {
        Some(container) => anchor.get(container),
        None => Some(anchor),
    }
}

fn insert(value: &mut Value, property: &DerivedProperty, component: &str, text: &str) {
    let anchor = anchor_of(property.section, component);
    let Some(anchor) = node_at_mut(value, &anchor) else {
        return;
    };
    let holder = match property.container {
        Some(container) => anchor.get_mut(container),
        None => Some(anchor),
    };
    let Some(Value::Mapping(map)) = holder else {
        return;
    };
    place(map, property, text);
}

fn place(map: &mut Mapping, property: &DerivedProperty, text: &str) {
    let key = Value::String(property.property.to_owned());
    let value = Value::String(text.to_owned());
    let Some(position) = map.keys().position(|k| k.as_str() == Some(property.after)) else {
        map.insert(key, value);
        return;
    };
    let tail: Vec<(Value, Value)> = map
        .iter()
        .skip(position + 1)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    for (existing, _) in &tail {
        map.remove(existing);
    }
    map.insert(key, value);
    for (existing, value) in tail {
        map.insert(existing, value);
    }
}

fn node_at<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split(PATH_SEPARATOR)
        .try_fold(value, |node, segment| node.get(segment))
}

fn node_at_mut<'a>(value: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    path.split(PATH_SEPARATOR)
        .try_fold(value, |node, segment| node.get_mut(segment))
}

#[cfg(test)]
mod tests;
