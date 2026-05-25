//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

const PROVIDER_AXIS: Axis = Axis {
    name: "provider",
    known: &["docker", "kminus"],
};

const COUNTRY_AXIS: Axis = Axis {
    name: "country",
    known: &["us", "es"],
};

const PREFER_DOCKER: &[&str] = &["docker"];
const PREFER_KMINUS: &[&str] = &["kminus"];
const PREFER_DOCKER_US: &[&str] = &["docker", "us"];

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
    let parsed = parse_filename("apiserver_docker.pod.yaml");
    assert_eq!(parsed.name, "apiserver");
    assert_eq!(parsed.tokens, vec!["docker".to_owned()]);
    assert_eq!(parsed.extensions, ".pod.yaml");
}

#[test]
fn parse_filename_with_multiple_tokens() {
    let parsed = parse_filename("apiserver_kminus_us.pod.yaml");
    assert_eq!(parsed.name, "apiserver");
    assert_eq!(parsed.tokens, vec!["kminus".to_owned(), "us".to_owned()]);
    assert_eq!(parsed.extensions, ".pod.yaml");
}

#[test]
fn parse_filename_with_token_and_no_extensions() {
    let parsed = parse_filename("apiserver_docker");
    assert_eq!(parsed.name, "apiserver");
    assert_eq!(parsed.tokens, vec!["docker".to_owned()]);
    assert_eq!(parsed.extensions, "");
}

#[test]
fn parse_filename_with_dash_in_name() {
    let parsed = parse_filename("api-server_docker.pod.yaml");
    assert_eq!(parsed.name, "api-server");
    assert_eq!(parsed.tokens, vec!["docker".to_owned()]);
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
        "apiserver_docker.pod.yaml",
        "apiserver_kminus_us.pod.yaml",
        "apiserver_docker",
    ];
    for case in cases {
        let parsed = parse_filename(case);
        assert_eq!(parsed.render(), case);
    }
}

#[test]
fn render_stripped_drops_all_tokens() {
    let parsed = parse_filename("apiserver_kminus_us.pod.yaml");
    assert_eq!(parsed.render_stripped(), "apiserver.pod.yaml");
}

#[test]
fn pick_winner_prefers_unprefixed_when_no_axes() {
    let candidates = vec![parse_filename("apiserver.pod.yaml")];
    assert_eq!(pick_winner(&candidates, &[], &[]), Some(0));
}

#[test]
fn pick_winner_rejects_candidate_when_no_axes_but_tokens_present() {
    let candidates = vec![parse_filename("apiserver_docker.pod.yaml")];
    assert_eq!(pick_winner(&candidates, &[], &[]), None);
}

#[test]
fn pick_winner_picks_matching_provider_over_plain() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_docker.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS], PREFER_DOCKER),
        Some(1)
    );
}

#[test]
fn pick_winner_falls_back_to_tokenless_when_provider_does_not_match() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_kminus.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS], PREFER_DOCKER),
        Some(0)
    );
}

#[test]
fn pick_winner_picks_longest_prefix() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_docker.pod.yaml"),
        parse_filename("apiserver_docker_us.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(
            &candidates,
            &[PROVIDER_AXIS, COUNTRY_AXIS],
            PREFER_DOCKER_US
        ),
        Some(2)
    );
}

#[test]
fn pick_winner_falls_back_when_country_does_not_match() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_docker.pod.yaml"),
        parse_filename("apiserver_docker_es.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(
            &candidates,
            &[PROVIDER_AXIS, COUNTRY_AXIS],
            PREFER_DOCKER_US
        ),
        Some(1)
    );
}

#[test]
fn pick_winner_rejects_excess_tokens() {
    let candidates = vec![parse_filename("apiserver_docker_us_extra.pod.yaml")];
    assert_eq!(
        pick_winner(
            &candidates,
            &[PROVIDER_AXIS, COUNTRY_AXIS],
            PREFER_DOCKER_US
        ),
        None
    );
}

#[test]
fn pick_winner_falls_back_to_tokenless_when_no_specific_match() {
    let candidates = vec![
        parse_filename("apiserver.pod.yaml"),
        parse_filename("apiserver_kminus.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS], PREFER_DOCKER),
        Some(0)
    );
}

#[test]
fn pick_winner_returns_none_when_no_candidate_eligible() {
    let candidates = vec![
        parse_filename("apiserver_kminus.pod.yaml"),
        parse_filename("apiserver_kminus_us.pod.yaml"),
    ];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS], PREFER_DOCKER),
        None
    );
}

#[test]
fn pick_winner_treats_misplaced_token_as_no_match() {
    let candidates = vec![parse_filename("apiserver_us.pod.yaml")];
    assert_eq!(
        pick_winner(
            &candidates,
            &[PROVIDER_AXIS, COUNTRY_AXIS],
            PREFER_DOCKER_US
        ),
        None
    );
}

#[test]
fn pick_winner_tokenless_is_universal_fallback_for_any_provider() {
    let candidates = vec![parse_filename("apiserver.pod.yaml")];
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS], PREFER_DOCKER),
        Some(0)
    );
    assert_eq!(
        pick_winner(&candidates, &[PROVIDER_AXIS], PREFER_KMINUS),
        Some(0)
    );
}

#[test]
fn group_by_key_groups_variants_together() {
    let filenames = vec![
        "apiserver.pod.yaml".to_owned(),
        "apiserver_docker.pod.yaml".to_owned(),
        "apiserver_kminus.pod.yaml".to_owned(),
        "inspector_docker.pod.yaml".to_owned(),
    ];
    let groups = group_by_key(&filenames);
    let apiserver_key = ("apiserver".to_owned(), ".pod.yaml".to_owned());
    let inspector_key = ("inspector".to_owned(), ".pod.yaml".to_owned());
    assert_eq!(groups.len(), 2);
    assert_eq!(groups.get(&apiserver_key).unwrap().len(), 3);
    assert_eq!(groups.get(&inspector_key).unwrap().len(), 1);
}
