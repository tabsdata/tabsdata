//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use std::path::PathBuf;

fn value(text: &str) -> Option<OsString> {
    Some(OsString::from(text))
}

fn list(entries: &[&str]) -> Option<OsString> {
    Some(join_paths(entries).expect("The entries were expected to be joinable"))
}

fn extended(path: &[&str], before: &[&str], after: &[&str]) -> Vec<PathBuf> {
    let result = extended_path(list(path), list(before), list(after));
    split_paths(&result.expect("The path was expected to be extended")).collect()
}

fn folders(entries: &[&str]) -> Vec<PathBuf> {
    entries.iter().map(PathBuf::from).collect()
}

#[test]
fn test_before_is_prepended_and_after_is_appended() {
    assert_eq!(
        extended(&["/usr/bin", "/bin"], &["/venv/bin"], &["/opt/bin"]),
        folders(&["/venv/bin", "/usr/bin", "/bin", "/opt/bin"])
    );
}

#[test]
fn test_empty_after_leaves_only_a_prefix() {
    assert_eq!(
        extended(&["/usr/bin", "/bin"], &["/venv/bin"], &[]),
        folders(&["/venv/bin", "/usr/bin", "/bin"])
    );
}

#[test]
fn test_absent_variables_leave_the_path_untouched() {
    assert_eq!(extended_path(value("/usr/bin"), None, None), None);
}

#[test]
fn test_empty_variables_leave_the_path_untouched() {
    assert_eq!(extended_path(value("/usr/bin"), value(""), value("")), None);
}

#[test]
fn test_extending_twice_is_idempotent() {
    let once = extended_path(
        list(&["/usr/bin", "/bin"]),
        value("/venv/bin"),
        value("/opt/bin"),
    )
    .expect("The path was expected to be extended");
    let twice = extended_path(Some(once.clone()), value("/venv/bin"), value("/opt/bin"))
        .expect("The path was expected to be extended");
    assert_eq!(once, twice);
}

#[test]
fn test_a_folder_already_present_is_not_duplicated() {
    assert_eq!(
        extended(&["/venv/bin", "/usr/bin"], &["/venv/bin"], &[]),
        folders(&["/venv/bin", "/usr/bin"])
    );
}

#[test]
fn test_an_absent_path_is_built_from_the_variables() {
    let result = extended_path(None, value("/venv/bin"), value("/opt/bin"))
        .expect("The path was expected to be extended");
    assert_eq!(
        split_paths(&result).collect::<Vec<PathBuf>>(),
        folders(&["/venv/bin", "/opt/bin"])
    );
}

#[test]
fn test_empty_entries_in_the_path_are_dropped() {
    assert_eq!(
        extended(&["/usr/bin", "", "/bin"], &["/venv/bin"], &[]),
        folders(&["/venv/bin", "/usr/bin", "/bin"])
    );
}

#[test]
fn test_several_folders_keep_their_declared_order() {
    assert_eq!(
        extended(
            &["/usr/bin"],
            &["/venv/bin", "/venv/extra"],
            &["/opt/one", "/opt/two"]
        ),
        folders(&[
            "/venv/bin",
            "/venv/extra",
            "/usr/bin",
            "/opt/one",
            "/opt/two"
        ])
    );
}
