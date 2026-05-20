//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

const PROVIDER_AXIS: Axis = Axis {
    name: "provider",
    preferred: "k8s",
    known: &["k8s", "k4s"],
};

const COUNTRY_AXIS: Axis = Axis {
    name: "country",
    preferred: "us",
    known: &["us", "es"],
};

#[test]
fn parse_filename_with_no_separators() {
    let parsed = parse_filename("runner");
    assert_eq!(parsed.name, "runner");
    assert!(parsed.tokens.is_empty());
    assert_eq!(parsed.extensions, "");
}

#[test]
fn parse_filename_with_only_extensions() {
    let parsed = parse_filename("apiserver.pod.yaml");
    assert_eq!(parsed.name, "apiserver");
    assert!(parsed.tokens.is_empty());
    assert_eq!(parsed.extensions, ".pod.yaml");
}

#[test]
fn parse_filename_with_one_token() {
    let parsed = parse_filename("apiserver_k8s.pod.yaml");
    assert_eq!(parsed.name, "apiserver");
    assert_eq!(parsed.tokens, vec!["k8s".to_owned()]);
    assert_eq!(parsed.extensions, ".pod.yaml");
}

#[test]
fn parse_filename_with_multiple_tokens() {
    let parsed = parse_filename("apiserver_k4s_us.pod.yaml");
    assert_eq!(parsed.name, "apiserver");
    assert_eq!(parsed.tokens, vec!["k4s".to_owned(), "us".to_owned()]);
    assert_eq!(parsed.extensions, ".pod.yaml");
}

#[test]
fn parse_filename_with_token_and_no_extensions() {
    let parsed = parse_filename("apiserver_k8s");
    assert_eq!(parsed.name, "apiserver");
    assert_eq!(parsed.tokens, vec!["k8s".to_owned()]);
    assert_eq!(parsed.extensions, "");
}

#[test]
fn parse_filename_with_dash_in_name() {
    let parsed = parse_filename("api-server_k8s.pod.yaml");
    assert_eq!(parsed.name, "api-server");
    assert_eq!(parsed.tokens, vec!["k8s".to_owned()]);
    assert_eq!(parsed.extensions, ".pod.yaml");
}

#[test]
fn parse_filename_with_empty_string() {
    let parsed = parse_filename("");
    assert_eq!(parsed.name, "");
    assert!(parsed.tokens.is_empty());
    assert_eq!(parsed.extensions, "");
}

#[test]
fn render_round_trip() {
    let cases = [
        "runner",
        "apiserver.pod.yaml",
        "apiserver_k8s.pod.yaml",
        "apiserver_k4s_us.pod.yaml",
        "apiserver_k8s",
    ];
    for case in cases {
        let parsed = parse_filename(case);
        assert_eq!(parsed.render(), case);
    }
}

#[test]
fn render_stripped_drops_all_tokens() {
    let parsed = parse_filename("apiserver_k4s_us.pod.yaml");
    assert_eq!(parsed.render_stripped(), "apiserver.pod.yaml");
}

#[test]
fn pick_winner_prefers_unprefixed_when_no_axes() {
    let candidates = vec![parse_filename("apiserver.pod.yaml")];
    assert_eq!(pick_winner(&candidates, &[]), Some(0));
}

#[test]
fn pick_winner_rejects_candidate_when_no_axes_but_tokens_present() {
    let candidates = vec![parse_filename("apiserver_k8s.pod.yaml")];
    assert_eq!(pick_winner(&candidates, &[]), None);
}

#[test]
fn pick_winner_picks_matching_provider_over_plain() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_k8s.pod.yaml"),
    ];
    assert_eq!(pick_winner(&candidates, &[PROVIDER_AXIS]), Some(1));
}

#[test]
fn pick_winner_picks_plain_when_no_match_for_provider() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_k4s.pod.yaml"),
    ];
    assert_eq!(pick_winner(&candidates, &[PROVIDER_AXIS]), Some(0));
}

#[test]
fn pick_winner_picks_longest_prefix() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_k8s.pod.yaml"),
        parse_filename("apiserver_k8s_us.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS, COUNTRY_AXIS]),
        Some(2)
    );
}

#[test]
fn pick_winner_falls_back_when_country_does_not_match() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_k8s.pod.yaml"),
        parse_filename("apiserver_k8s_es.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS, COUNTRY_AXIS]),
        Some(1)
    );
}

#[test]
fn pick_winner_rejects_excess_tokens() {
    let candidates = vec![parse_filename("apiserver_k8s_us_extra.pod.yaml")];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS, COUNTRY_AXIS]),
        None
    );
}

#[test]
fn pick_winner_returns_none_when_no_candidate_eligible() {
    let candidates = vec![
        parse_filename("apiserver_k4s.pod.yaml"),
        parse_filename("apiserver_k4s_us.pod.yaml"),
    ];
    assert_eq!(pick_winner(&candidates, &[PROVIDER_AXIS]), None);
}

#[test]
fn pick_winner_treats_misplaced_token_as_no_match() {
    let candidates = vec![parse_filename("apiserver_us.pod.yaml")];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS, COUNTRY_AXIS]),
        None
    );
}

#[test]
fn group_by_key_groups_variants_together() {
    let filenames = vec![
        "apiserver.pod.yaml".to_owned(),
        "apiserver_k8s.pod.yaml".to_owned(),
        "apiserver_k4s.pod.yaml".to_owned(),
        "inspector_k8s.pod.yaml".to_owned(),
    ];
    let groups = group_by_key(&filenames);
    let apiserver_key = ("apiserver".to_owned(), ".pod.yaml".to_owned());
    let inspector_key = ("inspector".to_owned(), ".pod.yaml".to_owned());
    assert_eq!(groups.len(), 2);
    assert_eq!(groups.get(&apiserver_key).unwrap().len(), 3);
    assert_eq!(groups.get(&inspector_key).unwrap().len(), 1);
}
