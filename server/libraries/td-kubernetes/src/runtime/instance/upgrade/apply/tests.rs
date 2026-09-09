//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::base::constants::{
    KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY, TABSDATA_REQUIREMENTS_LOCK_ASSET,
};
use crate::runtime::instance::upgrade::spec::NestChildrenIfFlat;
use crate::runtime::instance::upgrade::store::family_of;

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

#[test]
fn the_shipped_agent_pin_bump_rewrites_the_previous_requirement() {
    let root = std::path::PathBuf::from(format!(
        "{}/../../../../tabsdata-ee/variant/resources/kupgrade",
        env!("CARGO_MANIFEST_DIR")
    ));
    if !root.is_dir() {
        return;
    }
    let entry = "aiagent.req.txt";
    let spec = crate::runtime::instance::upgrade::catalog::load_spec(&root, "2.0.2")
        .expect("the ee 2.0.2 spec must load");
    let action = spec
        .configs
        .get(entry)
        .and_then(|group| group.get(entry))
        .unwrap_or_else(|| panic!("2.0.2 must carry a '{entry}' entry"));

    let previous = std::fs::read_to_string(
        root.join(crate::runtime::instance::upgrade::inventory::UPGRADES_FOLDER)
            .join("2.0.1")
            .join("configs")
            .join(entry),
    )
    .expect("the 2.0.1 requirement must be readable");

    let (rendered, touched) =
        modify("2.0.2", entry, &previous, action.changes()).expect("modifies");
    assert!(touched, "the shipped pattern must match 2.0.1: {previous}");
    assert!(
        rendered.contains("tabsdata-agent==2.0.2"),
        "the pin must be bumped: {rendered}"
    );
    assert!(
        !rendered.contains("tabsdata-agent==2.0.1"),
        "no stale pin may survive: {rendered}"
    );

    let (again, retouched) = modify("2.0.2", entry, &rendered, action.changes()).expect("modifies");
    assert!(!retouched, "re-running the hop must be a no-op: {again}");

    let (untouched, matched) =
        modify("2.0.2", entry, "tabsdata-agent==2X0X1\n", action.changes()).expect("modifies");
    assert!(
        !matched,
        "the dots must be escaped, not wildcards: {untouched}"
    );
}

fn sibling(leaf: &str, sibling: &str, value: serde_yaml::Value, placement: Placement) -> Operation {
    scoped(leaf, sibling, value, placement, Scope::Addressed)
}

fn scoped(
    leaf: &str,
    sibling: &str,
    value: serde_yaml::Value,
    placement: Placement,
    scope: Scope,
) -> Operation {
    Operation::InsertSiblingIfExisting(InsertSiblingIfExisting {
        leaf: leaf.to_owned(),
        sibling: sibling.to_owned(),
        value,
        placement,
        scope,
    })
}

const LABELLED: &str = "metadata:\n  labels:\n    tabsdata.com/tier: regular\n";

#[test]
fn a_sibling_lands_after_an_existing_leaf() {
    let (rendered, touched) = applied(
        LABELLED,
        &[sibling(
            "metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
        )],
    );
    assert!(touched);
    let tier = rendered.find("tabsdata.com/tier").expect("the anchor");
    let added = rendered.find("tabsdata.com/aiagent").expect("the sibling");
    assert!(
        tier < added,
        "the sibling must follow the anchor: {rendered}"
    );
}

#[test]
fn a_sibling_lands_before_an_existing_leaf() {
    let (rendered, touched) = applied(
        LABELLED,
        &[sibling(
            "metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::Before,
        )],
    );
    assert!(touched);
    let tier = rendered.find("tabsdata.com/tier").expect("the anchor");
    let added = rendered.find("tabsdata.com/aiagent").expect("the sibling");
    assert!(
        added < tier,
        "the sibling must precede the anchor: {rendered}"
    );
}

#[test]
fn an_absent_anchor_inserts_nothing() {
    let (rendered, touched) = applied(
        "metadata:\n  labels:\n    app: tabsdata\n",
        &[sibling(
            "metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
        )],
    );
    assert!(!touched);
    assert!(!rendered.contains("aiagent"));
}

#[test]
fn an_existing_sibling_is_left_alone() {
    let held =
        "metadata:\n  labels:\n    tabsdata.com/tier: regular\n    tabsdata.com/aiagent: false\n";
    let (rendered, touched) = applied(
        held,
        &[sibling(
            "metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
        )],
    );
    assert!(!touched);
    assert_eq!(rendered, held);
}

#[test]
fn inserting_a_sibling_twice_is_a_no_op() {
    let change = [sibling(
        "metadata.labels.\"tabsdata.com/tier\"",
        "tabsdata.com/aiagent",
        serde_yaml::Value::Bool(true),
        Placement::After,
    )];
    let (once, touched) = applied(LABELLED, &change);
    assert!(touched);
    let (twice, retouched) = applied(&once, &change);
    assert!(!retouched, "re-running must not double-insert: {twice}");
}

#[test]
fn a_missing_parent_inserts_nothing() {
    let (_, touched) = applied(
        "metadata:\n  name: srv-aiagent\n",
        &[sibling(
            "metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
        )],
    );
    assert!(!touched);
}

#[test]
fn a_repeated_anchor_elsewhere_is_not_a_second_insertion_point() {
    let held = "\
metadata:
  labels:
    tabsdata.com/tier: regular
spec:
  jobTemplate:
    metadata:
      labels:
        tabsdata.com/tier: ephemeral
    spec:
      template:
        metadata:
          labels:
            tabsdata.com/tier: ephemeral
";
    let (rendered, touched) = applied(
        held,
        &[sibling(
            "metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
        )],
    );
    assert!(touched);
    assert_eq!(
        rendered.matches("tabsdata.com/aiagent").count(),
        1,
        "only the addressed mapping may gain the sibling: {rendered}"
    );
    let doc: serde_yaml::Value = serde_yaml::from_str(&rendered).expect("valid yaml");
    assert_eq!(
        doc.get("metadata")
            .and_then(|node| node.get("labels"))
            .and_then(|node| node.get("tabsdata.com/aiagent")),
        Some(&serde_yaml::Value::Bool(true))
    );
}

const NESTED: &str = "\
metadata:
  labels:
    tabsdata.com/tier: regular
spec:
  jobTemplate:
    metadata:
      labels:
        tabsdata.com/tier: ephemeral
    spec:
      template:
        metadata:
          labels:
            tabsdata.com/tier: ephemeral
";

#[test]
fn an_everywhere_scope_reaches_every_repeated_anchor() {
    let (rendered, touched) = applied(
        NESTED,
        &[scoped(
            "\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
            Scope::Everywhere,
        )],
    );
    assert!(touched);
    assert_eq!(
        rendered.matches("tabsdata.com/aiagent").count(),
        3,
        "every holder of the anchor must gain the sibling: {rendered}"
    );
}

#[test]
fn an_everywhere_scope_is_idempotent() {
    let change = [scoped(
        "\"tabsdata.com/tier\"",
        "tabsdata.com/aiagent",
        serde_yaml::Value::Bool(true),
        Placement::After,
        Scope::Everywhere,
    )];
    let (once, touched) = applied(NESTED, &change);
    assert!(touched);
    let (twice, retouched) = applied(&once, &change);
    assert!(!retouched, "re-running must not double-insert: {twice}");
    assert_eq!(twice.matches("tabsdata.com/aiagent").count(), 3);
}

#[test]
fn an_everywhere_scope_adds_the_sibling_only_where_the_anchor_sits() {
    let (rendered, touched) = applied(
        NESTED,
        &[scoped(
            "tabsdata.com/absent",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
            Scope::Everywhere,
        )],
    );
    assert!(!touched);
    assert!(!rendered.contains("aiagent"));
}

#[test]
fn a_partially_labelled_document_gains_only_the_missing_siblings() {
    let held = "\
metadata:
  labels:
    tabsdata.com/tier: regular
    tabsdata.com/aiagent: true
spec:
  jobTemplate:
    metadata:
      labels:
        tabsdata.com/tier: ephemeral
";
    let (rendered, touched) = applied(
        held,
        &[scoped(
            "\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
            Scope::Everywhere,
        )],
    );
    assert!(touched);
    assert_eq!(rendered.matches("tabsdata.com/aiagent").count(), 2);
}

#[test]
fn an_addressed_scope_can_target_a_nested_anchor_over_the_first_one() {
    let (rendered, touched) = applied(
        NESTED,
        &[sibling(
            "spec.jobTemplate.metadata.labels.\"tabsdata.com/tier\"",
            "tabsdata.com/aiagent",
            serde_yaml::Value::Bool(true),
            Placement::After,
        )],
    );
    assert!(touched);
    assert_eq!(rendered.matches("tabsdata.com/aiagent").count(), 1);
    let doc: serde_yaml::Value = serde_yaml::from_str(&rendered).expect("valid yaml");
    assert!(
        doc.get("metadata")
            .and_then(|node| node.get("labels"))
            .and_then(|node| node.get("tabsdata.com/aiagent"))
            .is_none(),
        "the top-level anchor must be left alone: {rendered}"
    );
    assert_eq!(
        doc.get("spec")
            .and_then(|node| node.get("jobTemplate"))
            .and_then(|node| node.get("metadata"))
            .and_then(|node| node.get("labels"))
            .and_then(|node| node.get("tabsdata.com/aiagent")),
        Some(&serde_yaml::Value::Bool(true)),
        "the addressed nested anchor must gain the sibling: {rendered}"
    );
}

#[test]
fn the_shipped_ttl_bump_rewrites_the_previous_retention() {
    let spec = crate::runtime::instance::upgrade::spec::deserialize_spec(
        "2.0.2",
        include_str!(concat!(
            tm_workspace::workspace_root!(),
            "/variant/resources/kupgrade/upgrades/2.0.2.yaml"
        )),
    )
    .expect("the shipped 2.0.2 spec parses");
    let group = spec
        .pods
        .get("pymanager.yaml")
        .expect("pymanager is covered by the ttl bump");
    let action = group
        .get("pymanager_kminus.yaml")
        .expect("its kminus variant is covered");

    let before = "apiVersion: batch/v1\nkind: Job\nspec:\n  backoffLimit: 0\n  \
                  ttlSecondsAfterFinished: 1800\n";
    let (after, touched) =
        modify("2.0.2", "pods/pymanager.yaml", before, action.changes()).expect("modifies");
    assert!(touched, "a 1800 retention must be rewritten");
    assert!(
        after.contains("ttlSecondsAfterFinished: 3600"),
        "the retention must land on an hour: {after}"
    );

    let (again, retouched) =
        modify("2.0.2", "pods/pymanager.yaml", &after, action.changes()).expect("modifies");
    assert!(!retouched, "re-running the hop must be a no-op: {again}");
}

#[test]
fn the_requirements_lock_is_a_foreign_asset_taken_from_the_installed_package() {
    let foreign = FOREIGN_ASSETS
        .iter()
        .find(|foreign| foreign.asset() == TABSDATA_REQUIREMENTS_LOCK_ASSET)
        .expect("the requirements lock travels with the installed package, not the upgrade");
    assert!(matches!(
        foreign.origin(),
        crate::base::constants::ForeignAssetOrigin::TabsdataResource(_)
    ));
}

#[test]
fn a_foreign_asset_targets_the_configmap_the_pods_mount() {
    let family = &KPROFILE_ASSETS_FAMILY;
    let file = TABSDATA_REQUIREMENTS_LOCK_ASSET;
    let name = to_kube_resource_name(family.scope, id_from_file(file), family.kind, file)
        .expect("names the configmap");
    assert_eq!(
        name,
        "td-app-tabsdata-requirements-asset-tabsdata-requirements.txt"
    );
}

#[test]
fn a_foreign_asset_is_stamped_under_the_assets_family() {
    let entry = format!(
        "{}/{}",
        KPROFILE_ASSETS_FAMILY.folder, TABSDATA_REQUIREMENTS_LOCK_ASSET
    );
    let mut manifest = Manifest::initial_install(
        "2.0.0",
        Utc::now(),
        [(entry.clone(), "kprofile".to_owned())],
    );
    manifest.stamp_entry(&entry, "2.0.1", Utc::now(), true);
    assert_eq!(
        manifest
            .entries
            .get(&entry)
            .expect("the asset is tracked")
            .version,
        "2.0.1"
    );
}

fn nest(leaf: &str, under: &str) -> Operation {
    Operation::NestChildrenIfFlat(NestChildrenIfFlat {
        leaf: leaf.to_owned(),
        under: under.to_owned(),
    })
}

const FLAT_POOL: &str = "database_server:\n  pool:\n    min_connections: 1\n    max_connections: \
                          2\n    test_before_acquire: true\n";

#[test]
fn a_flat_block_nests_under_the_new_key() {
    let (rendered, touched) = applied(FLAT_POOL, &[nest("database_server.pool", "admin")]);
    assert!(touched);
    let document: serde_yaml::Value = serde_yaml::from_str(&rendered).expect("parses");
    let admin = document["database_server"]["pool"]["admin"].clone();
    assert_eq!(admin["min_connections"], serde_yaml::Value::from(1));
    assert_eq!(admin["max_connections"], serde_yaml::Value::from(2));
    assert_eq!(admin["test_before_acquire"], serde_yaml::Value::from(true));
    assert_eq!(
        document["database_server"]["pool"]
            .as_mapping()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn an_already_nested_block_is_left_alone() {
    let nested = "database_server:\n  pool:\n    admin:\n      min_connections: 1\n";
    let (rendered, touched) = applied(nested, &[nest("database_server.pool", "admin")]);
    assert!(!touched);
    assert_eq!(rendered, nested);
}

#[test]
fn a_missing_block_nests_nothing() {
    let (_, touched) = applied(FLAT_POOL, &[nest("database_server.absent", "admin")]);
    assert!(!touched);
}

#[test]
fn an_empty_block_nests_nothing() {
    let (_, touched) = applied(
        "database_server:\n  pool: {}\n",
        &[nest("database_server.pool", "admin")],
    );
    assert!(!touched);
}

fn shipped(relative: &str) -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn shipped_changes(group: &str, variant: &str) -> Vec<Operation> {
    let yaml = shipped("variant/resources/kupgrade/upgrades/2.1.0.yaml");
    let spec = crate::runtime::instance::upgrade::spec::deserialize_spec("2.1.0", &yaml)
        .expect("the shipped spec parses");
    spec.configs[group][variant].changes().to_vec()
}

fn parsed(yaml: &str) -> serde_yaml::Value {
    serde_yaml::from_str(yaml).expect("parses")
}

/// The 2.0.2 shape of `configs/apiserver.database.yaml`, as every variant shipped it.
const APISERVER_DATABASE_2_0_2: &str = "\
database:
  apiserver:
    postgres:
      password: ${{secretAuto:postgres_apiserver_password}}
    pool:
      min_connections: 5
      max_connections: 50
      acquire_timeout: 30
      max_lifetime: 3600
      idle_timeout: 60
      test_before_acquire: true
";

/// The 2.0.2 shape of `configs/dbmanager.database.yaml`, as the kminus variant shipped it.
const DBMANAGER_DATABASE_2_0_2: &str = "\
database_server:
  postgres:
    host: 127.0.0.1
    port: 2460
    database: postgres
    username: ${{secretAuto:postgres_system_user}}
    password: ${{secretAuto:postgres_system_password}}
    ssl_mode: prefer
    ssl_root_cert: null
  pool:
    min_connections: 1
    max_connections: 2
    acquire_timeout: 30
    max_lifetime: 1800
    idle_timeout: 30
    test_before_acquire: true
";

#[test]
fn the_shipped_spec_carries_a_2_0_2_apiserver_config_to_the_2_1_0_shape() {
    let changes = shipped_changes("apiserver.database.yaml", "apiserver.database_docker.yaml");
    let (rendered, touched) = applied(APISERVER_DATABASE_2_0_2, &changes);
    assert!(touched);
    assert_eq!(
        parsed(&rendered),
        parsed(&shipped(
            "variant/resources/kprofile/configs/apiserver.database_docker.yaml"
        ))
    );
}

#[test]
fn the_shipped_spec_carries_a_2_0_2_dbmanager_config_to_the_2_1_0_shape() {
    let changes = shipped_changes("dbmanager.database.yaml", "dbmanager.database_docker.yaml");
    let (rendered, touched) = applied(DBMANAGER_DATABASE_2_0_2, &changes);
    assert!(touched);
    let document = parsed(&rendered);
    let admin = document["database_server"]["pool"]["admin"].clone();
    assert_eq!(admin["min_connections"], serde_yaml::Value::from(1));
    assert_eq!(admin["max_connections"], serde_yaml::Value::from(1));
    assert_eq!(admin["max_lifetime"], serde_yaml::Value::from(1800));
    assert_eq!(
        document["database_server"]["postgres"]["host"],
        serde_yaml::Value::from("127.0.0.1"),
        "the staged values must survive the upgrade: {rendered}"
    );
    assert_eq!(
        document["database_server"]["postgres"]["port"],
        serde_yaml::Value::from(2460)
    );
}

#[test]
fn the_shipped_spec_is_idempotent_over_an_upgraded_config() {
    let changes = shipped_changes("apiserver.database.yaml", "apiserver.database_docker.yaml");
    let (once, _) = applied(APISERVER_DATABASE_2_0_2, &changes);
    let (twice, touched) = applied(&once, &changes);
    assert!(!touched);
    assert_eq!(once, twice);
}
