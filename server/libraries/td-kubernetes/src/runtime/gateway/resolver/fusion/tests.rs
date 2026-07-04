//
// Copyright 2026 Tabs Data Inc.
//

use super::*;

fn configs(entries: &[(&str, &str)]) -> RenderedConfigmap {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn bake_schema_secrets_rewrites_matched_leaves_to_tokens() {
    let schema = "directives:\n  - at: options.token\n    treat: secret\n";
    let config = "options:\n  token: baked\n  other: keep\n";
    let configs = configs(&[("apiserver.yamls", schema)]);
    let baked = bake_schema_secrets("apiserver.yaml", config, &configs).expect("bake");
    assert!(baked.contains("token: ${{secret}}"));
    assert!(baked.contains("other: keep"));
}

#[test]
fn bake_schema_secrets_is_noop_without_schema_file() {
    let config = "options:\n  token: baked\n";
    let configs = configs(&[]);
    let baked = bake_schema_secrets("apiserver.yaml", config, &configs).expect("bake");
    assert_eq!(baked, config);
}

#[test]
fn bake_schema_secrets_is_noop_for_empty_schema() {
    let schema = "other: 1\n";
    let config = "options:\n  token: baked\n";
    let configs = configs(&[("apiserver.yamls", schema)]);
    let baked = bake_schema_secrets("apiserver.yaml", config, &configs).expect("bake");
    assert_eq!(baked, config);
}

#[test]
fn bake_schema_secrets_ignores_non_yaml_stem() {
    let config = "plain content";
    let configs = configs(&[]);
    let baked = bake_schema_secrets("notes.txt", config, &configs).expect("bake");
    assert_eq!(baked, config);
}

fn write_family(root: &Path, family: &str, files: &[(&str, &str)]) {
    let folder = root.join(family);
    fs::create_dir_all(&folder).expect("create family folder");
    for (name, content) in files {
        fs::write(folder.join(name), content).expect("write family file");
    }
}

#[test]
fn compare_family_accepts_exact_match() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x"), ("b.yaml", "y")]);
    let canonical = configs(&[("a.yaml", "x"), ("b.yaml", "y")]);
    let result = compare_family(temp.path(), "configs", &canonical, false);
    assert!(result.is_ok());
}

#[test]
fn compare_family_rejects_extra_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x"), ("extra.yaml", "z")]);
    let canonical = configs(&[("a.yaml", "x")]);
    let error = compare_family(temp.path(), "configs", &canonical, false)
        .expect_err("extra file must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationExtraFile { .. }
    ));
}

#[test]
fn compare_family_rejects_missing_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x")]);
    let canonical = configs(&[("a.yaml", "x"), ("b.yaml", "y")]);
    let error = compare_family(temp.path(), "configs", &canonical, false)
        .expect_err("missing file must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationMissingFile { .. }
    ));
}

#[test]
fn compare_family_strict_rejects_modified_pod() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "pods", &[("worker.yaml", "changed")]);
    let canonical = configs(&[("worker.yaml", "canonical")]);
    let error = compare_family(temp.path(), "pods", &canonical, true)
        .expect_err("modified pod must be rejected");
    assert!(matches!(
        error,
        ResolverError::InitializationPodModified { .. }
    ));
}

#[test]
fn compare_family_strict_accepts_unmodified_pod() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "pods", &[("worker.yaml", "canonical")]);
    let canonical = configs(&[("worker.yaml", "canonical")]);
    let result = compare_family(temp.path(), "pods", &canonical, true);
    assert!(result.is_ok());
}

#[test]
fn read_flat_family_rejects_nested_subfolder() {
    let temp = tempfile::tempdir().expect("tempdir");
    let folder = temp.path().join("configs");
    fs::create_dir_all(folder.join("nested")).expect("create nested");
    let error =
        read_flat_family(&folder, "configs").expect_err("nested subfolder must be rejected");
    assert!(matches!(error, ResolverError::InitializationNotFlat { .. }));
}

#[test]
fn read_flat_family_lists_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    write_family(temp.path(), "configs", &[("a.yaml", "x"), ("b.yaml", "y")]);
    let files = read_flat_family(&temp.path().join("configs"), "configs").expect("read");
    assert!(files.contains("a.yaml"));
    assert!(files.contains("b.yaml"));
    assert_eq!(files.len(), 2);
}
