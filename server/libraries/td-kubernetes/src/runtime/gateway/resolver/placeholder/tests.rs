//
// Copyright 2026 Tabs Data Inc.
//

use super::*;
use crate::runtime::gateway::resolver::walker::{Placeholder, ResolveContext, Rule};

fn placeholder(kind: &str, category: Option<&str>, rule: Rule) -> Placeholder {
    Placeholder {
        kind: kind.to_owned(),
        category: category.map(str::to_owned),
        rule,
    }
}

fn context<'a>(
    placeholder: &'a Placeholder,
    path: &'a [String],
    init_value: Option<&'a str>,
    auto_value: Option<&'a str>,
    system: &'a SystemValues,
) -> ResolveContext<'a> {
    ResolveContext {
        path,
        occurrence: 0,
        placeholder,
        init_value,
        auto_value,
        system,
    }
}

const PATH: &[&str] = &["a", "b"];

fn path() -> Vec<String> {
    PATH.iter().map(|s| s.to_string()).collect()
}

#[test]
fn literal_resolve_prefers_init_over_auto() {
    let resolver = resolver_for("literal", "a.b").expect("resolver");
    let bar = placeholder("literal", Some("label"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, Some("from-init"), Some("from-auto"), &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    assert_eq!(resolution.rendered, "from-init");
}

#[test]
fn literal_resolve_falls_back_to_auto_when_init_empty() {
    let resolver = resolver_for("literal", "a.b").expect("resolver");
    let bar = placeholder("literal", Some("label"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, Some("from-auto"), &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    assert_eq!(resolution.rendered, "from-auto");
}

#[test]
fn literal_required_without_value_errors() {
    let resolver = resolver_for("literal", "a.b").expect("resolver");
    let bar = placeholder("literal", Some("label"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let result = resolver.resolve(&context);
    assert!(matches!(
        result,
        Err(ResolverError::MissingRequiredValue { .. })
    ));
}

#[test]
fn literal_auto_without_value_renders_empty() {
    let resolver = resolver_for("literalAuto", "a.b").expect("resolver");
    let bar = placeholder("literalAuto", Some("label"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    assert_eq!(resolution.rendered, "");
}

#[test]
fn literal_optional_without_value_renders_empty() {
    let resolver = resolver_for("literal", "a.b").expect("resolver");
    let bar = placeholder("literal", Some("label"), Rule::Optional);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    assert_eq!(resolution.rendered, "");
}

#[test]
fn env_resolve_keeps_token_at_creation() {
    let resolver = resolver_for("env", "a.b").expect("resolver");
    let bar = placeholder("env", Some("HOME"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    assert_eq!(resolution.rendered, "${{env:HOME}}");
}

#[test]
fn env_materialize_reads_process_variable() {
    let key = "TD_PLACEHOLDER_TEST_ENV";
    unsafe { std::env::set_var(key, "env-value") };

    let resolver = resolver_for("env", "a.b").expect("resolver");
    let bar = placeholder("env", Some(key), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let resolution = resolver.materialize(&context).expect("materialize");
    assert_eq!(resolution.rendered, "env-value");

    unsafe { std::env::remove_var(key) };
}

#[test]
fn env_materialize_missing_required_errors() {
    unsafe { std::env::remove_var("TD_PLACEHOLDER_TEST_MISSING") };

    let resolver = resolver_for("env", "a.b").expect("resolver");
    let bar = placeholder("env", Some("TD_PLACEHOLDER_TEST_MISSING"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let result = resolver.materialize(&context);
    assert!(matches!(
        result,
        Err(ResolverError::EnvironmentVariableNotFound { .. })
    ));
}

#[test]
fn secret_resolve_collects_init_value_and_retains_token() {
    let resolver = resolver_for("secret", "a.b").expect("resolver");
    let bar = placeholder("secret", None, Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, Some("topsecret"), None, &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    assert_eq!(resolution.rendered, "${{secret}}");
    let secret = resolution.secret.expect("secret entry");
    assert_eq!(secret.value, "topsecret");
}

#[test]
fn secret_required_without_value_errors() {
    let resolver = resolver_for("secret", "a.b").expect("resolver");
    let bar = placeholder("secret", None, Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let result = resolver.resolve(&context);
    assert!(matches!(
        result,
        Err(ResolverError::MissingRequiredValue { .. })
    ));
}

#[test]
fn secret_auto_generates_known_generator_value() {
    let resolver = resolver_for("secretAuto", "a.b").expect("resolver");
    let bar = placeholder("secretAuto", Some("postgres_system_user"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let resolution = resolver.resolve(&context).expect("resolve");
    let secret = resolution.secret.expect("secret entry");
    assert_eq!(secret.value, "postgres");
}

#[test]
fn secret_auto_unknown_generator_errors() {
    let resolver = resolver_for("secretAuto", "a.b").expect("resolver");
    let bar = placeholder("secretAuto", Some("not_a_generator"), Rule::Mandatory);
    let path = path();
    let system = SystemValues::default();
    let context = context(&bar, &path, None, None, &system);
    let result = resolver.resolve(&context);
    assert!(matches!(result, Err(ResolverError::UnknownGenerator { .. })));
}

#[test]
fn resolver_for_rejects_unknown_kind() {
    let result = resolver_for("bogus", "a.b");
    assert!(matches!(
        result,
        Err(ResolverError::UnknownPlaceholderKind { .. })
    ));
}

#[test]
fn resolver_for_rejects_unsupported_kind() {
    let result = resolver_for("config", "a.b");
    assert!(matches!(
        result,
        Err(ResolverError::UnsupportedPlaceholderKind { .. })
    ));
}
