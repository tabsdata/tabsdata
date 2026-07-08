//
// Copyright 2026 Tabs Data Inc.
//

use serde_yaml::Value;

use super::*;
use crate::base::error::gateway::ResolverError;

fn placeholder(kind: &str, category: Option<&str>, rule: Rule) -> Placeholder {
    Placeholder {
        kind: kind.to_owned(),
        category: category.map(str::to_owned),
        rule,
    }
}

#[test]
fn contains_placeholder_detects_presence() {
    assert!(contains_placeholder("${{literal}}"));
    assert!(contains_placeholder("prefix ${{system:port}} suffix"));
    assert!(!contains_placeholder("plain value"));
    assert!(!contains_placeholder("${ not a placeholder }"));
    assert!(!contains_placeholder("${{unterminated"));
}

#[test]
fn parse_bare_placeholder_kind_only() {
    let parsed = parse_bare_placeholder("${{literal}}").expect("bare");
    assert_eq!(parsed, placeholder("literal", None, Rule::Mandatory));
}

#[test]
fn parse_bare_placeholder_kind_and_category() {
    let parsed = parse_bare_placeholder("${{system:tdk_apiserver_public_port}}").expect("bare");
    assert_eq!(
        parsed,
        placeholder("system", Some("tdk_apiserver_public_port"), Rule::Mandatory)
    );
}

#[test]
fn parse_bare_placeholder_optional_rule() {
    let parsed = parse_bare_placeholder("${{env:HOME?}}").expect("bare");
    assert_eq!(parsed, placeholder("env", Some("HOME"), Rule::Optional));

    let parsed = parse_bare_placeholder("${{env?}}").expect("bare");
    assert_eq!(parsed, placeholder("env", None, Rule::Optional));
}

#[test]
fn parse_bare_placeholder_category_charset() {
    let parsed = parse_bare_placeholder("${{secret:a.b-c_d}}").expect("bare");
    assert_eq!(parsed.category.as_deref(), Some("a.b-c_d"));
}

#[test]
fn parse_bare_placeholder_rejects_embedded() {
    assert!(parse_bare_placeholder("prefix ${{literal}}").is_none());
    assert!(parse_bare_placeholder("${{literal}} suffix").is_none());
    assert!(parse_bare_placeholder("0.0.0.0:${{system:port}}").is_none());
    assert!(parse_bare_placeholder("").is_none());
    assert!(parse_bare_placeholder("plain").is_none());
}

#[test]
fn placeholder_occurrences_collects_in_order() {
    let occurrences = placeholder_occurrences("${{a}}:${{b:cat}}/${{c?}}");
    assert_eq!(occurrences.len(), 3);
    assert_eq!(occurrences[0], placeholder("a", None, Rule::Mandatory));
    assert_eq!(
        occurrences[1],
        placeholder("b", Some("cat"), Rule::Mandatory)
    );
    assert_eq!(occurrences[2], placeholder("c", None, Rule::Optional));
}

#[test]
fn placeholder_occurrences_keeps_duplicates() {
    let occurrences = placeholder_occurrences("${{secret}}${{secret}}");
    assert_eq!(occurrences.len(), 2);
}

#[test]
fn placeholder_occurrences_none_when_absent() {
    assert!(placeholder_occurrences("plain text").is_empty());
}

#[test]
fn token_round_trips() {
    for text in [
        "${{literal}}",
        "${{system:tdk_database_port}}",
        "${{env:HOME?}}",
        "${{env?}}",
    ] {
        let parsed = parse_bare_placeholder(text).expect("bare");
        assert_eq!(parsed.token(), text);
    }
}

#[test]
fn splice_placeholders_preserves_surrounding_text() {
    let rendered = splice_placeholders("0.0.0.0:${{system:port}}", |occurrence, placeholder| {
        assert_eq!(occurrence, 0);
        assert_eq!(placeholder.kind, "system");
        Ok("2457".to_owned())
    })
    .expect("splice");
    assert_eq!(rendered, "0.0.0.0:2457");
}

#[test]
fn splice_placeholders_counts_occurrences_left_to_right() {
    let rendered = splice_placeholders("${{a}}-${{b}}-${{c}}", |occurrence, _| {
        Ok(occurrence.to_string())
    })
    .expect("splice");
    assert_eq!(rendered, "0-1-2");
}

#[test]
fn splice_placeholders_no_placeholders_returns_input() {
    let rendered = splice_placeholders("plain", |_, _| Ok(String::new())).expect("splice");
    assert_eq!(rendered, "plain");
}

#[test]
fn splice_placeholders_propagates_closure_error() {
    let error = splice_placeholders("${{a}}", |_, _| {
        Err(ResolverError::ParseFailed {
            path: "p".to_owned(),
            cause: "boom".to_owned(),
        })
    });
    assert!(matches!(error, Err(ResolverError::ParseFailed { .. })));
}

#[derive(Default)]
struct RecordingVisitor {
    visits: Vec<(String, usize, Placeholder)>,
    render: String,
}

impl PlaceholderVisitor for RecordingVisitor {
    fn visit_placeholder(
        &mut self,
        path: &[String],
        occurrence: usize,
        placeholder: &Placeholder,
    ) -> Result<String, ResolverError> {
        self.visits
            .push((path.join(PATH_SEPARATOR), occurrence, placeholder.clone()));
        Ok(self.render.clone())
    }
}

fn walk_str(yaml: &str, render: &str) -> (Value, RecordingVisitor) {
    let mut value: Value = serde_yaml::from_str(yaml).expect("yaml");
    let mut visitor = RecordingVisitor {
        render: render.to_owned(),
        ..RecordingVisitor::default()
    };
    walk(&mut value, &mut visitor).expect("walk");
    (value, visitor)
}

#[test]
fn walk_visits_mapping_paths() {
    let (_, visitor) = walk_str("database:\n  password: ${{secret:pw}}\n", "X");
    assert_eq!(visitor.visits.len(), 1);
    assert_eq!(visitor.visits[0].0, "database.password");
    assert_eq!(visitor.visits[0].2.kind, "secret");
}

#[test]
fn walk_coerces_bare_placeholder_to_number() {
    let (value, _) = walk_str("port: ${{system:port}}\n", "2457");
    assert_eq!(value["port"], Value::Number(2457.into()));
}

#[test]
fn walk_coerces_bare_placeholder_to_bool() {
    let (value, _) = walk_str("flag: ${{literal:f}}\n", "true");
    assert_eq!(value["flag"], Value::Bool(true));
}

#[test]
fn walk_embedded_placeholder_stays_string() {
    let (value, _) = walk_str("address: 0.0.0.0:${{system:port}}\n", "2457");
    assert_eq!(value["address"], Value::String("0.0.0.0:2457".to_owned()));
}

#[test]
fn walk_scalar_list_addressed_by_index() {
    let (_, visitor) = walk_str("hosts:\n- ${{literal:a}}\n- ${{literal:b}}\n", "X");
    let paths: Vec<&str> = visitor.visits.iter().map(|v| v.0.as_str()).collect();
    assert_eq!(paths, vec!["hosts.0", "hosts.1"]);
}

#[test]
fn walk_keyed_map_list_addressed_by_identity() {
    let yaml = "mounts:\n\
                - id: ROOT\n  uri: ${{system:root}}\n\
                - id: BUNDLES\n  uri: ${{system:bundles}}\n";
    let (_, visitor) = walk_str(yaml, "X");
    let paths: Vec<&str> = visitor.visits.iter().map(|v| v.0.as_str()).collect();
    assert_eq!(paths, vec!["mounts.ROOT.uri", "mounts.BUNDLES.uri"]);
}

#[test]
fn walk_sequence_without_placeholders_is_untouched() {
    let (_, visitor) = walk_str("hosts:\n- a\n- b\n", "X");
    assert!(visitor.visits.is_empty());
}

fn walk_error(yaml: &str) -> ResolverError {
    let mut value: Value = serde_yaml::from_str(yaml).expect("yaml");
    let mut visitor = RecordingVisitor {
        render: "X".to_owned(),
        ..RecordingVisitor::default()
    };
    walk(&mut value, &mut visitor).expect_err("expected walk error")
}

#[test]
fn walk_mixed_sequence_falls_back_to_positional() {
    let (_, visitor) = walk_str("items:\n- plain\n- id: A\n  v: ${{secret}}\n", "X");
    let paths: Vec<&str> = visitor.visits.iter().map(|v| v.0.as_str()).collect();
    assert_eq!(paths, vec!["items.1.v"]);
}

#[test]
fn walk_rejects_first_key_mismatch() {
    let yaml = "items:\n\
                - id: A\n  v: ${{secret}}\n\
                - name: B\n  v: ${{secret}}\n";
    let error = walk_error(yaml);
    assert!(matches!(
        error,
        ResolverError::SequenceFirstKeyMismatch { .. }
    ));
}

#[test]
fn walk_rejects_duplicate_first_values() {
    let yaml = "items:\n\
                - id: A\n  v: ${{secret}}\n\
                - id: A\n  v: ${{secret}}\n";
    let error = walk_error(yaml);
    assert!(matches!(
        error,
        ResolverError::SequenceDuplicateFirstValues { .. }
    ));
}

#[test]
fn walk_rejects_placeholder_as_identity() {
    let yaml = "items:\n\
                - id: ${{literal:x}}\n  v: ${{secret}}\n";
    let error = walk_error(yaml);
    assert!(matches!(error, ResolverError::PlaceholderAsIdentity { .. }));
}
