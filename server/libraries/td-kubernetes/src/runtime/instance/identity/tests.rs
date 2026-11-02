//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use indoc::indoc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use super::*;
use crate::base::constants::{POD_YAML_COMPONENT_LABEL, TIER_LABEL};

const ROLE_ARN_ANNOTATION: &str = "eks.amazonaws.com/role-arn";
const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/tabsdata-apiserver";
const DESCRIPTION_ANNOTATION: &str = "tabsdata.com/description";

fn account(component: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: ObjectMeta {
            name: Some(format!("aut-{component}")),
            labels: Some(BTreeMap::from([
                (POD_YAML_COMPONENT_LABEL.to_owned(), component.to_owned()),
                (TIER_LABEL.to_owned(), "authorization".to_owned()),
            ])),
            annotations: Some(BTreeMap::from([(
                DESCRIPTION_ANNOTATION.to_owned(),
                "Identity".to_owned(),
            )])),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn annotation<'a>(account: &'a ServiceAccount, key: &str) -> Option<&'a str> {
    account
        .metadata
        .annotations
        .as_ref()?
        .get(key)
        .map(String::as_str)
}

#[test]
fn the_component_annotations_are_added_to_its_service_account() {
    let mut account = account("apiserver");
    apply_identity(
        indoc! {"
            pods:
              service:
                apiserver:
                  service_account:
                    annotations:
                      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/tabsdata-apiserver
        "},
        &mut account,
    )
    .expect("applies");
    assert_eq!(annotation(&account, ROLE_ARN_ANNOTATION), Some(ROLE_ARN));
    assert_eq!(
        annotation(&account, DESCRIPTION_ANNOTATION),
        Some("Identity")
    );
}

#[test]
fn another_component_annotations_are_ignored() {
    let mut account = account("apiserver");
    apply_identity(
        indoc! {"
            pods:
              service:
                aiagent:
                  service_account:
                    annotations:
                      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/tabsdata-aiagent
        "},
        &mut account,
    )
    .expect("applies");
    assert_eq!(annotation(&account, ROLE_ARN_ANNOTATION), None);
}

#[test]
fn empty_annotation_values_are_skipped() {
    let mut account = account("apiserver");
    apply_identity(
        indoc! {r#"
            pods:
              service:
                apiserver:
                  service_account:
                    annotations:
                      eks.amazonaws.com/role-arn: ""
        "#},
        &mut account,
    )
    .expect("applies");
    assert_eq!(annotation(&account, ROLE_ARN_ANNOTATION), None);
}

#[test]
fn a_config_without_service_account_settings_leaves_the_account_untouched() {
    let mut account = account("apiserver");
    let before = account.metadata.annotations.clone();
    apply_identity(
        indoc! {"
            pods:
              service:
                apiserver:
                  resources:
                    cpu: \"2\"
        "},
        &mut account,
    )
    .expect("applies");
    assert_eq!(account.metadata.annotations, before);
}

#[test]
fn an_account_without_annotations_gets_them_created() {
    let mut account = account("apiserver");
    account.metadata.annotations = None;
    apply_identity(
        indoc! {"
            pods:
              service:
                apiserver:
                  service_account:
                    annotations:
                      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/tabsdata-apiserver
        "},
        &mut account,
    )
    .expect("applies");
    assert_eq!(annotation(&account, ROLE_ARN_ANNOTATION), Some(ROLE_ARN));
}

#[test]
fn a_malformed_config_fails_to_apply() {
    let mut account = account("apiserver");
    let outcome = apply_identity(
        indoc! {"
            pods:
              service:
                apiserver:
                  service_account:
                    annotations: [not, a, map]
        "},
        &mut account,
    );
    assert!(outcome.is_err());
}
