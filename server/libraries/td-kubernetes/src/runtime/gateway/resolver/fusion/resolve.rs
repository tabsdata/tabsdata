//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use serde_yaml::Value;

use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::kind::{PlaceholderKind, Substitution};
use crate::runtime::gateway::resolver::placeholder::{SystemValues, resolver_for};
use crate::runtime::gateway::resolver::walker::{
    PATH_SEPARATOR, Phase, Placeholder, ResolveContext, SecretEntry, contains_placeholder,
    parse_bare_placeholder, placeholder_occurrences, splice_placeholders,
};

const CONFIG_LABEL: &str = "<config>";

pub(super) struct ResolveOutcome {
    pub rendered: String,
    pub secrets: Vec<SecretEntry>,
}

pub(super) fn resolve_config_file(
    template: &str,
    init: &str,
    system: &SystemValues,
    auto: &HashMap<String, String>,
) -> Result<ResolveOutcome, ResolverError> {
    let template_value: Value =
        serde_yaml::from_str(template).map_err(|cause| ResolverError::ParseFailed {
            path: CONFIG_LABEL.to_owned(),
            cause: cause.to_string(),
        })?;
    let init_value: Value =
        serde_yaml::from_str(init).map_err(|cause| ResolverError::ParseFailed {
            path: CONFIG_LABEL.to_owned(),
            cause: cause.to_string(),
        })?;

    let mut session = ResolveSession {
        system,
        auto,
        secrets: Vec::new(),
        values: HashMap::new(),
    };
    let mut path: Vec<String> = Vec::new();
    let rendered_value = session.resolve_node(&template_value, &init_value, &mut path)?;

    let rendered =
        serde_yaml::to_string(&rendered_value).map_err(|cause| ResolverError::ParseFailed {
            path: CONFIG_LABEL.to_owned(),
            cause: cause.to_string(),
        })?;
    Ok(ResolveOutcome {
        rendered,
        secrets: session.secrets,
    })
}

struct ResolveSession<'a> {
    system: &'a SystemValues,
    auto: &'a HashMap<String, String>,
    secrets: Vec<SecretEntry>,
    values: HashMap<String, String>,
}

impl ResolveSession<'_> {
    fn resolve_node(
        &mut self,
        template: &Value,
        init: &Value,
        path: &mut Vec<String>,
    ) -> Result<Value, ResolverError> {
        match template {
            Value::String(template_text) => self.resolve_scalar(template_text, init, path),
            Value::Mapping(template_map) => {
                let Value::Mapping(init_map) = init else {
                    return Err(self.structure_mismatch(path));
                };
                let mut rendered = serde_yaml::Mapping::new();
                for (key, template_child) in template_map {
                    let segment = scalar_key(key);
                    let Some(init_child) = init_map.get(key) else {
                        return Err(self.structure_mismatch_at(path, &segment));
                    };
                    path.push(segment);
                    let rendered_child = self.resolve_node(template_child, init_child, path)?;
                    path.pop();
                    rendered.insert(key.clone(), rendered_child);
                }
                Ok(Value::Mapping(rendered))
            }
            Value::Sequence(template_items) => {
                let Value::Sequence(init_items) = init else {
                    return Err(self.structure_mismatch(path));
                };
                if is_keyed_map_sequence(template_items) {
                    self.resolve_keyed_sequence(template_items, init_items, path)
                } else {
                    if template_items.len() != init_items.len() {
                        return Err(self.structure_mismatch(path));
                    }
                    let mut rendered = Vec::with_capacity(template_items.len());
                    for (index, (template_item, init_item)) in
                        template_items.iter().zip(init_items).enumerate()
                    {
                        path.push(index.to_string());
                        let rendered_item = self.resolve_node(template_item, init_item, path)?;
                        path.pop();
                        rendered.push(rendered_item);
                    }
                    Ok(Value::Sequence(rendered))
                }
            }
            _ => Ok(init.clone()),
        }
    }

    fn resolve_keyed_sequence(
        &mut self,
        template_items: &[Value],
        init_items: &[Value],
        path: &mut Vec<String>,
    ) -> Result<Value, ResolverError> {
        let displayed_path = path.join(PATH_SEPARATOR);
        let first_template =
            template_items
                .first()
                .ok_or_else(|| ResolverError::SequenceEmptyItem {
                    path: displayed_path.clone(),
                    item_index: 0,
                })?;
        let shape_key =
            first_key(first_template).ok_or_else(|| ResolverError::SequenceEmptyItem {
                path: displayed_path.clone(),
                item_index: 0,
            })?;

        let mut templates_by_identity: HashMap<String, &Value> =
            HashMap::with_capacity(template_items.len());
        for (item_index, template_item) in template_items.iter().enumerate() {
            let identity = item_identity(template_item, &shape_key, &displayed_path, item_index)?;
            templates_by_identity.insert(identity, template_item);
        }

        let mut seen: HashMap<String, usize> = HashMap::with_capacity(init_items.len());
        let mut rendered = Vec::with_capacity(init_items.len());
        for (item_index, init_item) in init_items.iter().enumerate() {
            let identity = item_identity(init_item, &shape_key, &displayed_path, item_index)?;
            if let Some(prior_index) = seen.get(&identity) {
                return Err(ResolverError::SequenceDuplicateFirstValues {
                    path: displayed_path.clone(),
                    value: identity,
                    first_index: *prior_index,
                    second_index: item_index,
                });
            }
            seen.insert(identity.clone(), item_index);

            let template_item = templates_by_identity
                .get(&identity)
                .copied()
                .unwrap_or(first_template);

            path.push(identity);
            let rendered_item = self.resolve_node(template_item, init_item, path)?;
            path.pop();
            rendered.push(rendered_item);
        }
        Ok(Value::Sequence(rendered))
    }

    fn resolve_scalar(
        &mut self,
        template_text: &str,
        init: &Value,
        path: &mut Vec<String>,
    ) -> Result<Value, ResolverError> {
        if !contains_placeholder(template_text) {
            return Ok(init.clone());
        }
        match parse_bare_placeholder(template_text) {
            Some(placeholder) => self.resolve_bare(&placeholder, init, path),
            None => self.resolve_embedded(template_text, init, path),
        }
    }

    fn resolve_bare(
        &mut self,
        placeholder: &Placeholder,
        init: &Value,
        path: &[String],
    ) -> Result<Value, ResolverError> {
        let displayed_path = path.join(PATH_SEPARATOR);
        let resolver = resolver_for(&placeholder.kind, &displayed_path)?;
        let kind = resolver.kind();
        let init_text = scalar_text(init, &displayed_path)?;

        match kind.behavior() {
            Substitution::Fixed => {
                self.expect_verbatim(placeholder, init_text, &displayed_path)?;
                let rendered = self.fixed_value(placeholder, kind, path, &displayed_path)?;
                self.record_value(placeholder, &displayed_path, &rendered)?;
                Ok(scalar_value(rendered))
            }
            Substitution::Inlined => {
                let context = self.context(placeholder, init_text, path);
                let resolution = resolver.resolve(&context)?;
                self.record_value(placeholder, &displayed_path, &resolution.rendered)?;
                Ok(scalar_value(resolution.rendered))
            }
            Substitution::Retained => {
                let context = self.context(placeholder, init_text, path);
                let resolution = resolver.resolve(&context)?;
                if let Some(secret) = resolution.secret {
                    self.secrets.push(secret);
                }
                Ok(Value::String(resolution.rendered))
            }
        }
    }

    fn resolve_embedded(
        &mut self,
        template_text: &str,
        init: &Value,
        path: &[String],
    ) -> Result<Value, ResolverError> {
        let displayed_path = path.join(PATH_SEPARATOR);
        for placeholder in placeholder_occurrences(template_text) {
            let resolver = resolver_for(&placeholder.kind, &displayed_path)?;
            if resolver.kind().behavior() != Substitution::Fixed {
                return Err(ResolverError::EmbeddedRetainedPlaceholder {
                    path: displayed_path,
                    token: placeholder.token(),
                });
            }
        }
        let init_text = scalar_text(init, &displayed_path)?;
        self.expect_verbatim_text(template_text, init_text, &displayed_path)?;

        let rendered = splice_placeholders(template_text, |_, placeholder| {
            let resolver = resolver_for(&placeholder.kind, &displayed_path)?;
            let kind = resolver.kind();
            self.fixed_value(placeholder, kind, path, &displayed_path)
        })?;
        Ok(Value::String(rendered))
    }

    fn fixed_value(
        &mut self,
        placeholder: &Placeholder,
        kind: PlaceholderKind,
        path: &[String],
        displayed_path: &str,
    ) -> Result<String, ResolverError> {
        match kind.phase() {
            Phase::Creation => {
                let context = self.context(placeholder, None, path);
                let resolver = resolver_for(&placeholder.kind, displayed_path)?;
                Ok(resolver.resolve(&context)?.rendered)
            }
            Phase::Execution => Ok(placeholder.token()),
        }
    }

    fn context<'b>(
        &'b self,
        placeholder: &'b Placeholder,
        init_text: Option<&'b str>,
        path: &'b [String],
    ) -> ResolveContext<'b> {
        let auto_value = placeholder
            .category
            .as_deref()
            .and_then(|category| self.auto.get(category))
            .map(String::as_str);
        ResolveContext {
            path,
            occurrence: 0,
            placeholder,
            init_value: init_text,
            auto_value,
            system: self.system,
        }
    }

    fn expect_verbatim(
        &self,
        placeholder: &Placeholder,
        init_text: Option<&str>,
        path: &str,
    ) -> Result<(), ResolverError> {
        self.expect_verbatim_text(&placeholder.token(), init_text, path)
    }

    fn expect_verbatim_text(
        &self,
        expected: &str,
        init_text: Option<&str>,
        path: &str,
    ) -> Result<(), ResolverError> {
        match init_text {
            Some(text) if text == expected => Ok(()),
            Some(text) => Err(ResolverError::NonEditablePlaceholderModified {
                path: path.to_owned(),
                expected: expected.to_owned(),
                found: text.to_owned(),
            }),
            None => Err(ResolverError::NonEditablePlaceholderModified {
                path: path.to_owned(),
                expected: expected.to_owned(),
                found: String::new(),
            }),
        }
    }

    fn record_value(
        &mut self,
        placeholder: &Placeholder,
        path: &str,
        rendered: &str,
    ) -> Result<(), ResolverError> {
        let identity = placeholder_identity(placeholder);
        match self.values.get(&identity) {
            Some(existing) if existing == rendered => Ok(()),
            Some(existing) => Err(ResolverError::DivergingPlaceholderValue {
                identity,
                path: path.to_owned(),
                first: existing.clone(),
                second: rendered.to_owned(),
            }),
            None => {
                self.values.insert(identity, rendered.to_owned());
                Ok(())
            }
        }
    }

    fn structure_mismatch(&self, path: &[String]) -> ResolverError {
        ResolverError::StructureMismatch {
            path: path.join(PATH_SEPARATOR),
        }
    }

    fn structure_mismatch_at(&self, path: &[String], segment: &str) -> ResolverError {
        let mut full = path.to_vec();
        full.push(segment.to_owned());
        ResolverError::StructureMismatch {
            path: full.join(PATH_SEPARATOR),
        }
    }
}

fn is_keyed_map_sequence(template_items: &[Value]) -> bool {
    !template_items.is_empty()
        && template_items
            .iter()
            .all(|item| matches!(item, Value::Mapping(_)))
        && template_items.iter().any(node_contains_placeholder)
}

fn item_identity(
    item: &Value,
    shape_key: &str,
    path: &str,
    item_index: usize,
) -> Result<String, ResolverError> {
    let Value::Mapping(map) = item else {
        return Err(ResolverError::SequenceMixedItems {
            path: path.to_owned(),
            item_index,
        });
    };
    let (key_node, value_node) =
        map.iter()
            .next()
            .ok_or_else(|| ResolverError::SequenceEmptyItem {
                path: path.to_owned(),
                item_index,
            })?;
    let key = scalar_key(key_node);
    if key != shape_key {
        return Err(ResolverError::SequenceFirstKeyMismatch {
            path: path.to_owned(),
            first_key: shape_key.to_owned(),
            item_index,
            other_key: key,
        });
    }
    identity_value(value_node, path, item_index)
}

fn first_key(item: &Value) -> Option<String> {
    match item {
        Value::Mapping(map) => map.iter().next().map(|(key, _)| scalar_key(key)),
        _ => None,
    }
}

fn identity_value(value: &Value, path: &str, item_index: usize) -> Result<String, ResolverError> {
    match value {
        Value::String(text) if contains_placeholder(text) => {
            Err(ResolverError::PlaceholderAsIdentity {
                path: path.to_owned(),
            })
        }
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        _ => Err(ResolverError::SequenceFirstValueNonScalar {
            path: path.to_owned(),
            item_index,
        }),
    }
}

fn node_contains_placeholder(node: &Value) -> bool {
    match node {
        Value::String(text) => contains_placeholder(text),
        Value::Mapping(map) => map.values().any(node_contains_placeholder),
        Value::Sequence(items) => items.iter().any(node_contains_placeholder),
        _ => false,
    }
}

fn placeholder_identity(placeholder: &Placeholder) -> String {
    match &placeholder.category {
        Some(category) => format!("{}:{}", placeholder.kind, category),
        None => placeholder.kind.clone(),
    }
}

fn scalar_text<'a>(value: &'a Value, path: &str) -> Result<Option<&'a str>, ResolverError> {
    match value {
        Value::String(text) => Ok(Some(text.as_str())),
        Value::Null => Ok(None),
        _ => Err(ResolverError::StructureMismatch {
            path: path.to_owned(),
        }),
    }
}

fn scalar_value(rendered: String) -> Value {
    if let Ok(integer) = rendered.parse::<i64>() {
        return Value::Number(integer.into());
    }
    if let Ok(unsigned) = rendered.parse::<u64>() {
        return Value::Number(unsigned.into());
    }
    match rendered.as_str() {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => Value::String(rendered),
    }
}

fn scalar_key(key: &Value) -> String {
    match key {
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::gateway::resolver::placeholder::SystemValues;

    #[test]
    fn secret_auto_collects_generated_value_and_retains_token() {
        let config = "\
database:
  system:
    username: ${{secretAuto:postgres_system_user}}
    password: ${{secretAuto:postgres_password}}
";
        let system = SystemValues::default();
        let auto = HashMap::new();
        let outcome = resolve_config_file(config, config, &system, &auto).expect("resolve");

        assert!(
            outcome
                .rendered
                .contains("${{secretAuto:postgres_system_user}}")
        );

        let by_key: std::collections::BTreeMap<_, _> = outcome
            .secrets
            .iter()
            .map(|entry| (entry.key.as_str(), entry.value.as_str()))
            .collect();
        let user = by_key
            .get("DATABASE__SYSTEM__USERNAME")
            .expect("username secret collected");
        assert_eq!(*user, "postgres");
        let password = by_key
            .get("DATABASE__SYSTEM__PASSWORD")
            .expect("password secret collected");
        assert!(
            !password.contains("${"),
            "password must be a generated value, not the token: {password}"
        );
    }
}
