//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn the_python_cache_folder_is_skipped() {
    assert!(is_python_cache(Path::new("assets/__pycache__")));
    assert!(is_hidden_or_system(Path::new("assets/__pycache__")));
}

#[test]
fn compiled_python_files_are_skipped() {
    assert!(is_python_cache(Path::new("assets/venvbuilder.pyc")));
    assert!(is_python_cache(Path::new("assets/venvbuilder.pyo")));
    assert!(is_hidden_or_system(Path::new("assets/venvbuilder.pyc")));
}

#[test]
fn profile_assets_are_kept() {
    for asset in [
        "assets/venvbuilder.py",
        "assets/venvloader.sh",
        "assets/requirements-td.txt",
        "pods/apiserver_docker.yaml",
    ] {
        assert!(!is_python_cache(Path::new(asset)), "{asset}");
        assert!(!is_hidden_or_system(Path::new(asset)), "{asset}");
    }
}

#[test]
fn hidden_files_are_still_skipped() {
    assert!(is_hidden_or_system(Path::new(".settings.json")));
    assert!(!is_python_cache(Path::new(".settings.json")));
}
