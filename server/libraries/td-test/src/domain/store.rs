//
// Copyright 2026 Tabsdata Inc.
//

//! Resolve a system/domain *name* to its descriptor file path.
//!
//! Port of `tabsdatax.itest.tdksystems2.store`: systems live under
//! `<backends-dir>/systems2` and domains under `<backends-dir>/domains2`, where
//! the backends dir is `$TD_BACKENDS_DIR` (else `~/.tabsdatax`). The `2` suffix
//! isolates the v2 store from the retired v1 `systems` / `domains` dirs.

use std::path::{Path, PathBuf};

use super::Role;

pub const ENV_BACKENDS_DIR: &str = "TD_BACKENDS_DIR";
const ENV_HOME: &str = "HOME";
pub const DEFAULT_BACKENDS_DIR: &str = ".tabsdatax";
pub const SYSTEMS_SUBDIR: &str = "systems2";
pub const DOMAINS_SUBDIR: &str = "domains2";

pub fn backends_dir() -> PathBuf {
    match std::env::var(ENV_BACKENDS_DIR) {
        Ok(dir) if !dir.is_empty() => expand_user(&dir),
        _ => home_dir().join(DEFAULT_BACKENDS_DIR),
    }
}

fn home_dir() -> PathBuf {
    match std::env::var(ENV_HOME) {
        Ok(home) if !home.is_empty() => PathBuf::from(home),
        _ => homedir::my_home()
            .ok()
            .flatten()
            .expect("cannot resolve the home directory holding the tdx descriptor store"),
    }
}

fn expand_user(dir: &str) -> PathBuf {
    match dir.strip_prefix('~') {
        Some(rest) => home_dir().join(rest.trim_start_matches(['/', '\\'])),
        None => PathBuf::from(dir),
    }
}

pub fn system_dir() -> PathBuf {
    backends_dir().join(SYSTEMS_SUBDIR)
}

pub fn domain_dir() -> PathBuf {
    backends_dir().join(DOMAINS_SUBDIR)
}

pub fn dir_of(role: Role) -> PathBuf {
    match role {
        Role::System => system_dir(),
        Role::Domain => domain_dir(),
    }
}

pub fn path_of(role: Role, name: &str) -> PathBuf {
    dir_of(role).join(name)
}

/// The backend names present in `role`'s store dir, sorted. Empty when the dir
/// does not exist — the store is only populated once tdx has provisioned.
pub fn stored_names(role: Role) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir_of(role)) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    names.sort();
    names
}

pub fn is_descriptor(path: &Path) -> bool {
    path.is_file() && std::fs::read_to_string(path).is_ok_and(|text| super::crypto::is_pem(&text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_user() {
        let home = home_dir();
        assert_eq!(expand_user("~/x"), home.join("x"));
        assert_eq!(expand_user("~"), home);
        assert_eq!(expand_user("/abs/x"), PathBuf::from("/abs/x"));
    }

    /// The store must resolve against the developer's real home. `td-common`
    /// mocks `get_home_dir` to a per-test scratch dir under `mock-env`, which
    /// would silently point every lookup at an empty store.
    #[test]
    fn test_home_is_not_the_mocked_test_home() {
        assert_ne!(home_dir(), td_common::env::get_home_dir());
        assert!(!home_dir().starts_with(td_common::testdir::root().join("target")));
    }

    /// `$HOME` wins over the passwd entry, matching Python's `expanduser` — the
    /// writer and the reader have to agree on which home holds the store.
    #[test]
    fn test_home_prefers_the_home_env_var() {
        temp_env::with_var("HOME", Some("/tmp/a-home"), || {
            assert_eq!(home_dir(), PathBuf::from("/tmp/a-home"));
            assert_eq!(expand_user("~/x"), PathBuf::from("/tmp/a-home/x"));
        });
        temp_env::with_var("HOME", Some(""), || {
            assert_eq!(home_dir(), homedir::my_home().unwrap().unwrap());
        });
    }

    #[test]
    fn test_subdirs() {
        assert!(system_dir().ends_with(SYSTEMS_SUBDIR));
        assert!(domain_dir().ends_with(DOMAINS_SUBDIR));
        assert_eq!(dir_of(Role::System), system_dir());
        assert_eq!(dir_of(Role::Domain), domain_dir());
        assert_eq!(
            path_of(Role::Domain, "landing"),
            domain_dir().join("landing")
        );
    }

    #[test]
    fn test_default_backends_dir() {
        temp_env::with_var_unset(ENV_BACKENDS_DIR, || {
            assert_eq!(backends_dir(), home_dir().join(DEFAULT_BACKENDS_DIR));
        });
    }

    #[test]
    fn test_backends_dir_from_env() {
        temp_env::with_var(ENV_BACKENDS_DIR, Some("/tmp/bd"), || {
            assert_eq!(backends_dir(), PathBuf::from("/tmp/bd"));
        });
        temp_env::with_var(ENV_BACKENDS_DIR, Some(""), || {
            assert_eq!(backends_dir(), home_dir().join(DEFAULT_BACKENDS_DIR));
        });
    }

    #[test]
    fn test_stored_names_missing_dir() {
        temp_env::with_var(ENV_BACKENDS_DIR, Some("/nonexistent/tdx/store"), || {
            assert!(stored_names(Role::Domain).is_empty());
        });
    }
}
