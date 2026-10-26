//
// Copyright 2026 Tabsdata Inc.
//

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use ta_apiserver::router::{AuthClass, Position, RouterStates, mount_router, registered};
use td_services::Services;
use te_services::ExtendedServices;

const SCHEMA_REF: &str = "#/components/schemas/";

fn positions() -> BTreeMap<String, Position> {
    registered()
        .into_iter()
        .flat_map(|r| {
            r.modes.iter().map(move |mode| Position {
                server: r.server,
                version: r.version,
                scope: r.scope,
                mode: *mode,
                class: AuthClass::of(r.auth),
            })
        })
        .map(|p| (format!("{p:?}"), p))
        .collect()
}

fn dangling_refs(doc: &Value) -> BTreeMap<String, BTreeSet<String>> {
    let schemas = doc
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .map(|s| s.keys().cloned().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    let mut dangling = BTreeMap::new();
    walk(doc, "", &schemas, &mut dangling);
    dangling
}

fn walk(
    node: &Value,
    path: &str,
    schemas: &BTreeSet<String>,
    dangling: &mut BTreeMap<String, BTreeSet<String>>,
) {
    match node {
        Value::Object(map) => {
            if let Some(target) = map.get("$ref").and_then(Value::as_str)
                && let Some(name) = target.strip_prefix(SCHEMA_REF)
                && !schemas.contains(name)
            {
                dangling
                    .entry(name.to_string())
                    .or_default()
                    .insert(path.to_string());
            }
            for (key, value) in map {
                walk(value, &format!("{path}/{key}"), schemas, dangling);
            }
        }
        Value::Array(items) => {
            for (i, value) in items.iter().enumerate() {
                walk(value, &format!("{path}/{i}"), schemas, dangling);
            }
        }
        _ => {}
    }
}

/// utoipa drops the schema references inside shapes it inlines (tuples), so
/// a `$ref` can point at a schema no derive registered.
#[test]
fn every_schema_ref_resolves() {
    let states = RouterStates::default()
        .with(Services::default())
        .with(ExtendedServices::default());
    let mut failures = Vec::new();
    for (label, position) in positions() {
        let (_, api) = mount_router(position, &states).split_for_parts();
        let doc = serde_json::to_value(api).unwrap();
        for (name, paths) in dangling_refs(&doc) {
            let first = paths.iter().next().unwrap();
            failures.push(format!(
                "{label}: `{name}` is referenced but not in components.schemas \
                 ({} place(s), first at {first})",
                paths.len()
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
