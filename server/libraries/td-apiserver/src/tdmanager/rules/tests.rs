//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use std::path::PathBuf;
use td_kubernetes::runtime::instance::upgrade::catalog::load_inventory;
use td_kubernetes::runtime::instance::upgrade::replay::Sources;
use td_kubernetes::runtime::instance::upgrade::rules::load_rules;

struct Rule(&'static str);

#[async_trait]
impl UpgradeRule for Rule {
    fn id(&self) -> &'static str {
        self.0
    }

    async fn apply(&self, _context: &RuleContext) -> Result<(), TdError> {
        Ok(())
    }
}

static FIRST: Rule = Rule("first");
static SECOND: Rule = Rule("second");
static SECOND_AGAIN: Rule = Rule("second");

fn ids(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

fn resolved(registry: &[&'static dyn UpgradeRule], requested: &[&str]) -> Vec<&'static str> {
    match resolve_in(registry.iter().copied(), &ids(requested)) {
        Ok(rules) => rules.iter().map(|rule| rule.id()).collect(),
        Err(error) => panic!("expected the rules to resolve: {error}"),
    }
}

fn refusal(registry: &[&'static dyn UpgradeRule], requested: &[&str]) -> String {
    match resolve_in(registry.iter().copied(), &ids(requested)) {
        Ok(_) => panic!("expected the rules to be refused"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn resolves_in_the_requested_order() {
    assert_eq!(
        resolved(&[&FIRST, &SECOND], &["second", "first"]),
        ["second", "first"]
    );
}

#[test]
fn resolves_nothing_when_nothing_is_requested() {
    assert!(resolved(&[&FIRST], &[]).is_empty());
}

#[test]
fn an_unknown_rule_is_refused() {
    let message = refusal(&[&FIRST], &["first", "missing"]);
    assert!(message.contains("'missing' is not registered"), "{message}");
}

#[test]
fn a_rule_requested_twice_is_refused() {
    let message = refusal(&[&FIRST], &["first", "first"]);
    assert!(message.contains("requested more than once"), "{message}");
}

#[test]
fn a_rule_registered_twice_is_refused() {
    let message = refusal(&[&SECOND, &SECOND_AGAIN], &["second"]);
    assert!(message.contains("registered more than once"), "{message}");
}

#[test]
fn every_shipped_rule_is_registered() {
    let mut repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..3 {
        repository.pop();
    }
    let kupgrade = Sources::of(&repository).kupgrade;
    let inventory = load_inventory(&kupgrade).expect("the shipped inventory loads");
    for entry in inventory.versions.iter().filter(|entry| entry.upgrader) {
        let ids = load_rules(&kupgrade, &entry.version).expect("the shipped rules load");
        if let Err(error) = resolve(&ids) {
            panic!(
                "the rules of version '{}' are not all registered: {error}",
                entry.version
            );
        }
    }
}
