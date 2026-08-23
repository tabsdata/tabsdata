//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn operation(leaf: &str, from: serde_yaml::Value, to: serde_yaml::Value) -> Operation {
    Operation::ModifyLeafValueIf(ModifyLeafValueIf {
        leaf: leaf.to_owned(),
        from,
        to,
    })
}

fn applied(current: &str, changes: &[Operation]) -> (String, bool) {
    modify("2.0.1", "configs/a.yaml", current, changes).expect("modifies")
}

#[test]
fn a_matching_leaf_is_replaced() {
    let (rendered, touched) = applied(
        "pods:\n  global:\n    envs:\n      UV_INDEX: ''\n",
        &[operation(
            "pods.global.envs.UV_INDEX",
            serde_yaml::Value::from(""),
            serde_yaml::Value::from("https://pypi.org/simple/"),
        )],
    );
    assert!(touched);
    assert!(rendered.contains("https://pypi.org/simple/"));
}

#[test]
fn a_leaf_holding_another_value_is_left_alone() {
    let (rendered, touched) = applied(
        "pods:\n  global:\n    envs:\n      UV_INDEX: mine\n",
        &[operation(
            "pods.global.envs.UV_INDEX",
            serde_yaml::Value::from(""),
            serde_yaml::Value::from("https://pypi.org/simple/"),
        )],
    );
    assert!(!touched);
    assert!(rendered.contains("mine"));
}

#[test]
fn a_missing_leaf_is_left_alone() {
    let (_, touched) = applied(
        "pods:\n  global:\n    envs: {}\n",
        &[operation(
            "pods.global.envs.UV_INDEX",
            serde_yaml::Value::from(""),
            serde_yaml::Value::from("x"),
        )],
    );
    assert!(!touched);
}

#[test]
fn a_missing_branch_is_left_alone() {
    let (_, touched) = applied(
        "other: 1\n",
        &[operation(
            "pods.global.envs.UV_INDEX",
            serde_yaml::Value::from(""),
            serde_yaml::Value::from("x"),
        )],
    );
    assert!(!touched);
}

#[test]
fn a_null_leaf_is_matched_and_replaced() {
    let (rendered, touched) = applied(
        "pods:\n  global:\n    envs:\n      UV_INDEX:\n",
        &[operation(
            "pods.global.envs.UV_INDEX",
            serde_yaml::Value::Null,
            serde_yaml::Value::from("set"),
        )],
    );
    assert!(touched);
    assert!(rendered.contains("set"));
}

#[test]
fn a_leaf_can_be_set_to_null() {
    let (rendered, touched) = applied(
        "pods:\n  global:\n    envs:\n      UV_INDEX: keep\n",
        &[operation(
            "pods.global.envs.UV_INDEX",
            serde_yaml::Value::from("keep"),
            serde_yaml::Value::Null,
        )],
    );
    assert!(touched);
    assert!(!rendered.contains("keep"));
}

#[test]
fn numbers_and_booleans_are_matched_by_type() {
    let (rendered, touched) = applied(
        "pool:\n  max_connections: 50\n  test_before_acquire: true\n",
        &[
            operation(
                "pool.max_connections",
                serde_yaml::Value::from(50),
                serde_yaml::Value::from(100),
            ),
            operation(
                "pool.test_before_acquire",
                serde_yaml::Value::from(true),
                serde_yaml::Value::from(false),
            ),
        ],
    );
    assert!(touched);
    assert!(rendered.contains("100"));
    assert!(rendered.contains("false"));
}

#[test]
fn every_change_is_attempted_even_when_one_does_not_match() {
    let (rendered, touched) = applied(
        "a: 1\nb: 2\n",
        &[
            operation("a", serde_yaml::Value::from(9), serde_yaml::Value::from(8)),
            operation("b", serde_yaml::Value::from(2), serde_yaml::Value::from(3)),
        ],
    );
    assert!(touched);
    assert!(rendered.contains("a: 1"));
    assert!(rendered.contains("b: 3"));
}

#[test]
fn an_unparseable_document_is_an_error() {
    let outcome = modify(
        "2.0.1",
        "configs/a.yaml",
        "a: [unclosed",
        &[Operation::ModifyLeafValueIf(ModifyLeafValueIf {
            leaf: "a".to_owned(),
            from: serde_yaml::Value::from("before"),
            to: serde_yaml::Value::from("after"),
        })],
    );
    assert!(outcome.is_err());
}

#[test]
fn a_document_with_no_changes_is_left_untouched() {
    let (rendered, touched) = modify("2.0.1", "configs/a.yaml", "a: [unclosed", &[]).expect("noop");
    assert_eq!(rendered, "a: [unclosed");
    assert!(!touched);
}

#[test]
fn every_family_folder_resolves() {
    for family in [
        &KPROFILE_ASSETS_FAMILY,
        &KPROFILE_CONFIGS_FAMILY,
        &KPROFILE_PODS_FAMILY,
    ] {
        assert_eq!(
            family_of(family.folder).expect("known").folder,
            family.folder
        );
    }
}

#[test]
fn an_unknown_family_folder_is_an_error() {
    assert!(family_of("secrets").is_err());
}

#[test]
fn a_quoted_segment_addresses_a_dotted_key() {
    let (rendered, touched) = applied(
        "metadata:\n  labels:\n    app.kubernetes.io/version: \"2.0.0\"\n",
        &[operation(
            "metadata.labels.\"app.kubernetes.io/version\"",
            serde_yaml::Value::from("2.0.0"),
            serde_yaml::Value::from("2.0.1"),
        )],
    );
    assert!(touched);
    assert!(rendered.contains("2.0.1"));
}

#[test]
fn an_unquoted_dotted_key_is_not_reached() {
    let (_, touched) = applied(
        "metadata:\n  labels:\n    app.kubernetes.io/version: \"2.0.0\"\n",
        &[operation(
            "metadata.labels.app.kubernetes.io/version",
            serde_yaml::Value::from("2.0.0"),
            serde_yaml::Value::from("x"),
        )],
    );
    assert!(!touched);
}

#[test]
fn quoting_is_optional_for_plain_segments() {
    assert_eq!(split_leaf("a.b.c"), vec!["a", "b", "c"]);
    assert_eq!(split_leaf("a.\"b.c\".d"), vec!["a", "b.c", "d"]);
    assert_eq!(split_leaf("\"only.one\""), vec!["only.one"]);
}

#[test]
fn discarding_absent_editable_folders_is_a_no_op() {
    let discarded = crate::runtime::instance::upgrade::hop::discard_editable_folders(
        "an-instance-that-does-not-exist",
    )
    .expect("no folders to discard");
    assert!(discarded.is_empty());
}

fn text(pattern: &str, replacement: &str, before: Option<&str>, after: Option<&str>) -> Operation {
    Operation::ModifyTextIf(ModifyTextIf {
        pattern: pattern.to_owned(),
        replacement: replacement.to_owned(),
        before: before.map(str::to_owned),
        after: after.map(str::to_owned),
    })
}

#[test]
fn a_text_change_rewrites_a_container_name() {
    let (rendered, touched) = applied(
        "spec:\n  containers:\n  - name: 1000-dbcreate\n    image: /dev/null\n",
        &[text(
            "1000-dbcreate",
            "1000-db${{system:tdk_dbmanager_mode}}",
            None,
            None,
        )],
    );
    assert!(touched);
    assert!(rendered.contains("1000-db${{system:tdk_dbmanager_mode}}"));
}

#[test]
fn boundaries_anchor_a_pattern_without_consuming_it() {
    let document = "args:\n    - db\n    - create\nother:\n    - create\n";
    let (rendered, touched) = applied(
        document,
        &[text(
            "- create",
            "- ${{system:tdk_dbmanager_mode}}",
            Some("- db\n    "),
            None,
        )],
    );
    assert!(touched);
    assert!(
        rendered.contains("- db"),
        "the boundary must survive the rewrite: {rendered}"
    );
    assert!(
        rendered.contains("${{system:tdk_dbmanager_mode}}"),
        "the anchored occurrence must be rewritten: {rendered}"
    );
    assert!(
        rendered.contains("other:\n    - create"),
        "the unanchored occurrence must survive: {rendered}"
    );
}

#[test]
fn a_pattern_matching_nothing_leaves_the_text_alone() {
    let (rendered, touched) = applied("a: 1\n", &[text("absent", "present", None, None)]);
    assert!(!touched);
    assert_eq!(rendered, "a: 1\n");
}

#[test]
fn text_and_structural_changes_compose() {
    let (rendered, touched) = applied(
        "metadata:\n  labels:\n    app.kubernetes.io/version: \"2.0.0\"\nspec:\n  containers:\n  - name: 1000-dbcreate\n",
        &[
            text("1000-dbcreate", "1000-dbupgrade", None, None),
            operation(
                "metadata.labels.\"app.kubernetes.io/version\"",
                serde_yaml::Value::from("2.0.0"),
                serde_yaml::Value::from("2.0.1"),
            ),
        ],
    );
    assert!(touched);
    assert!(rendered.contains("1000-dbupgrade"));
    assert!(rendered.contains("2.0.1"));
}

#[test]
fn an_invalid_pattern_is_an_error() {
    let outcome = modify(
        "2.0.1",
        "pods/a.yaml",
        "a: 1\n",
        &[text("[unclosed", "x", None, None)],
    );
    assert!(outcome.is_err());
}

#[test]
fn operations_apply_in_the_declared_order_across_types() {
    let document = "metadata:\n  labels:\n    version: \"2.0.0\"\n";
    let changes = [
        Operation::ModifyLeafValueIf(ModifyLeafValueIf {
            leaf: "metadata.labels.version".to_owned(),
            from: serde_yaml::Value::from("2.0.0"),
            to: serde_yaml::Value::from("staged"),
        }),
        Operation::ModifyTextIf(ModifyTextIf {
            pattern: "staged".to_owned(),
            replacement: "settled".to_owned(),
            before: None,
            after: None,
        }),
    ];
    let (rendered, touched) = modify("2.0.1", "pods/a.yaml", document, &changes).expect("modified");
    assert!(touched);
    assert!(
        rendered.contains("settled"),
        "the text change must see the leaf change: {rendered}"
    );

    let reversed = [changes[1].clone(), changes[0].clone()];
    let (rendered, _) = modify("2.0.1", "pods/a.yaml", document, &reversed).expect("modified");
    assert!(
        rendered.contains("staged") && !rendered.contains("settled"),
        "reversing the order must change the outcome: {rendered}"
    );
}
