//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn a_missing_link_is_created() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target");
    let link = temp.path().join("link");
    fs::create_dir_all(&target).unwrap();

    relink(&link, &target).unwrap();

    assert!(is_symlink(&link));
    assert!(link.is_dir());
    assert_eq!(fs::read_link(&link).unwrap(), target);
}

#[test]
fn an_existing_link_is_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    let link = temp.path().join("link");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&second).unwrap();

    relink(&link, &first).unwrap();
    relink(&link, &second).unwrap();

    assert_eq!(fs::read_link(&link).unwrap(), second);
}

#[test]
fn replacing_a_link_keeps_its_target() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    let link = temp.path().join("link");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&second).unwrap();
    fs::write(first.join("keep.txt"), "keep").unwrap();

    relink(&link, &first).unwrap();
    relink(&link, &second).unwrap();

    assert!(first.join("keep.txt").is_file());
}

#[test]
fn an_existing_folder_is_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target");
    let link = temp.path().join("link");
    fs::create_dir_all(&target).unwrap();
    fs::create_dir_all(&link).unwrap();
    fs::write(link.join("stale.txt"), "stale").unwrap();

    relink(&link, &target).unwrap();

    assert!(is_symlink(&link));
    assert_eq!(fs::read_link(&link).unwrap(), target);
}

#[test]
fn a_plain_path_is_not_a_link() {
    let temp = tempfile::tempdir().unwrap();
    let folder = temp.path().join("folder");
    fs::create_dir_all(&folder).unwrap();

    assert!(!is_symlink(&folder));
    assert!(!is_symlink(&temp.path().join("missing")));
}
