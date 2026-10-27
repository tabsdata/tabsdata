//
// Copyright 2026 Tabsdata Inc.
//

//! The tdd services test backends read: a backend's descriptor and the store's
//! names, each from tdd's JSON events.
//!
//! Only tdd knows the descriptors; tests never open the store. `tdd-exe` is
//! found the way cargo make and the Python fixtures find it (`$TDD_DIR`, else
//! `~/.tdd`) and reads its key from `$_TDD_CRYPTO_KEY`; `$TDD_SYSTEM_DIR` /
//! `$TDD_DOMAIN_DIR` point it at another store.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;
use serde_json::Value;

/// Which family of the tdd store a backend belongs to. `system_orders` and
/// `domain_orders` are distinct backends that happen to share a name, so every
/// lookup is keyed by the pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    Domain,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::System => "system",
            Role::Domain => "domain",
        }
    }

    /// tdd's store dir for this family, when not its default one.
    pub fn store_env(self) -> &'static str {
        match self {
            Role::System => "TDD_SYSTEM_DIR",
            Role::Domain => "TDD_DOMAIN_DIR",
        }
    }

    /// Comma-separated `name`s of this family that tests skip even when present.
    pub fn skip_env(self) -> &'static str {
        match self {
            Role::System => "TD_TEST_SKIP_SYSTEMS",
            Role::Domain => "TD_TEST_SKIP_DOMAINS",
        }
    }

    /// Whether `name` is listed in [`Role::skip_env`].
    pub fn is_skipped(self, name: &str) -> bool {
        std::env::var(self.skip_env())
            .unwrap_or_default()
            .split(',')
            .any(|listed| listed.trim() == name && !name.is_empty())
    }

    /// The manifest this run provisioned this family from, as `cargo make`
    /// exports it: what the run offers on its platform.
    pub fn manifest_env(self) -> &'static str {
        match self {
            Role::System => "TD_TEST_SYSTEMS_MANIFEST",
            Role::Domain => "TD_TEST_DOMAINS_MANIFEST",
        }
    }

    /// Whether the run offers `name`: always, when no manifest is declared in
    /// [`Role::manifest_env`]; otherwise only when the manifest lists it. A
    /// declared manifest that cannot be read is an error, never "offers all".
    pub fn is_offered(self, name: &str) -> Result<bool, String> {
        let Some(path) = std::env::var(self.manifest_env())
            .ok()
            .map(|path| path.trim().to_string())
            .filter(|path| !path.is_empty())
        else {
            return Ok(true);
        };
        #[derive(Deserialize)]
        struct Entry {
            name: String,
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("${} = '{path}' cannot be read: {e}", self.manifest_env()))?;
        let entries: Vec<Entry> = serde_yaml::from_str(&content).map_err(|e| {
            format!(
                "${} = '{path}' is not a {} manifest: {e}",
                self.manifest_env(),
                self.as_str()
            )
        })?;
        Ok(entries.iter().any(|entry| entry.name == name))
    }
}

pub const ENV_TDD_DIR: &str = "TDD_DIR";
pub const ENV_CRYPTO_KEY: &str = "_TDD_CRYPTO_KEY";
const ENV_HOME: &str = "HOME";
const DEFAULT_TDD_DIR: &str = ".tdd";

const EVENT_OK: &str = "ok";
const EVENT_ERROR: &str = "error";

/// A system or domain descriptor, as `tdd ... info --format json` prints it.
/// Fields tdd adds over time are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct Descriptor {
    pub role: Role,
    /// Slug, e.g. `s3`.
    pub system: String,
    pub name: String,
    #[serde(default)]
    pub sys_name: String,
    /// Entity-container name in the system.
    #[serde(default)]
    pub external_name: String,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub data: Option<String>,
    /// Each seeded entity to its row count. For a SQL domain `{table: rows}`;
    /// for a file domain `{staged_file_path: rows}`.
    #[serde(default)]
    pub entities: BTreeMap<String, i64>,
}

/// The developer's real home. `td-common` mocks `get_home_dir` to a per-test
/// scratch dir, which would point every lookup at a tdd that is not there.
fn home_dir() -> PathBuf {
    match std::env::var(ENV_HOME) {
        Ok(home) if !home.is_empty() => PathBuf::from(home),
        _ => homedir::my_home()
            .ok()
            .flatten()
            .expect("cannot resolve the home directory holding tdd"),
    }
}

fn expand_user(dir: &str) -> PathBuf {
    match dir.strip_prefix('~') {
        Some(rest) => home_dir().join(rest.trim_start_matches(['/', '\\'])),
        None => PathBuf::from(dir),
    }
}

pub fn exe() -> PathBuf {
    let tdd_dir = match std::env::var(ENV_TDD_DIR) {
        Ok(dir) if !dir.is_empty() => expand_user(&dir),
        _ => home_dir().join(DEFAULT_TDD_DIR),
    };
    let venv = tdd_dir.join("tdd-venvs").join("tdd");
    if cfg!(windows) {
        venv.join("Scripts").join("tdd-exe.exe")
    } else {
        venv.join("bin").join("tdd-exe")
    }
}

fn name_opt(role: Role) -> &'static str {
    match role {
        Role::System => "--sys",
        Role::Domain => "--domain",
    }
}

fn listing(role: Role) -> &'static str {
    match role {
        Role::System => "systems",
        Role::Domain => "domains",
    }
}

/// Run `tdd-exe <role> <verb> [args] --format json` and return its ok event.
fn run(role: Role, verb: &str, args: &[&str]) -> Result<Value, String> {
    let exe = exe();
    if !exe.is_file() {
        return Err(format!(
            "Tool tdd is not installed at {}; run 'cargo make permit' to install it.",
            exe.display()
        ));
    }
    let command = format!("tdd {} {verb}", role.as_str());
    let output = Command::new(&exe)
        .arg(role.as_str())
        .arg(verb)
        .args(args)
        .args(["--format", "json"])
        .output()
        .map_err(|e| format!("Command '{command}' could not be started: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let last = stdout
        .lines()
        .rev()
        .filter(|line| line.starts_with('{'))
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| {
            matches!(
                event.get("event").and_then(Value::as_str),
                Some(EVENT_OK | EVENT_ERROR)
            )
        });
    let code = output.status.code().unwrap_or(-1);
    match last {
        None => Err(format!(
            "Command '{command}' ended without an ok or error event (exit code {code}):\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        )),
        Some(event) if event["event"] == EVENT_ERROR || !output.status.success() => Err(format!(
            "Command '{command}' failed (exit code {code}): {}",
            event["message"].as_str().unwrap_or_default()
        )),
        Some(event) => Ok(event),
    }
}

/// The names of the store's systems or domains.
pub fn names(role: Role) -> Result<Vec<String>, String> {
    let ok = run(role, "list", &[])?;
    let mut names: Vec<String> = ok[listing(role)]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry["name"].as_str().map(str::to_string))
        .collect();
    names.sort();
    Ok(names)
}

/// The descriptor of system or domain `name`.
pub fn descriptor(role: Role, name: &str) -> Result<Descriptor, String> {
    let ok = run(role, "info", &[name_opt(role), name])?;
    let mut found: Vec<Descriptor> =
        serde_json::from_value(ok["descriptors"].clone()).map_err(|e| {
            format!(
                "Command 'tdd {} info' printed a malformed descriptor: {e}",
                role.as_str()
            )
        })?;
    match found.len() {
        1 => Ok(found.remove(0)),
        count => Err(format!(
            "Command 'tdd {} info' returned {count} descriptors for '{name}', expected one",
            role.as_str()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Descriptor {
        serde_json::from_str(json).expect("a descriptor in tdd's JSON shape")
    }

    #[test]
    fn test_role_names() {
        assert_eq!(Role::System.as_str(), "system");
        assert_eq!(Role::Domain.as_str(), "domain");
        assert_eq!(Role::System.store_env(), "TDD_SYSTEM_DIR");
        assert_eq!(Role::Domain.store_env(), "TDD_DOMAIN_DIR");
        assert_eq!(Role::System.skip_env(), "TD_TEST_SKIP_SYSTEMS");
        assert_eq!(Role::Domain.skip_env(), "TD_TEST_SKIP_DOMAINS");
        assert_eq!(Role::System.manifest_env(), "TD_TEST_SYSTEMS_MANIFEST");
        assert_eq!(Role::Domain.manifest_env(), "TD_TEST_DOMAINS_MANIFEST");
    }

    #[test]
    fn test_skip_by_name() {
        let env = Role::Domain.skip_env();
        temp_env::with_var_unset(env, || {
            assert!(!Role::Domain.is_skipped("landing"));
        });
        temp_env::with_var(env, Some("other, landing ,more"), || {
            assert!(Role::Domain.is_skipped("landing"));
            assert!(!Role::Domain.is_skipped("land"));
            assert!(!Role::System.is_skipped("landing"));
        });
        // An empty list must not swallow every backend.
        temp_env::with_var(env, Some(""), || {
            assert!(!Role::Domain.is_skipped("landing"));
        });
    }

    #[test]
    fn test_offered_by_the_manifest() {
        let manifest = std::env::temp_dir().join(format!(
            "td-tdd-offered-{}-{}.yaml",
            std::process::id(),
            td_common::id::id()
        ));
        std::fs::write(
            &manifest,
            "#\n# A comment\n#\n\n- name: landing\n  sys: s3\n- name: other\n  sys: gcs\n  model: world\n",
        )
        .unwrap();
        let env = Role::Domain.manifest_env();
        temp_env::with_var_unset(env, || {
            assert!(Role::Domain.is_offered("anything").unwrap());
        });
        let declared = [
            (env, Some(manifest.to_str().unwrap())),
            (Role::System.manifest_env(), None),
        ];
        temp_env::with_vars(declared, || {
            assert!(Role::Domain.is_offered("landing").unwrap());
            assert!(Role::Domain.is_offered("other").unwrap());
            assert!(!Role::Domain.is_offered("land").unwrap());
            assert!(Role::System.is_offered("land").unwrap());
        });
        temp_env::with_var(env, Some(" "), || {
            assert!(Role::Domain.is_offered("anything").unwrap());
        });
        temp_env::with_var(env, Some("/no/such/manifest.yaml"), || {
            let error = Role::Domain.is_offered("landing").unwrap_err();
            assert!(error.contains("TD_TEST_DOMAINS_MANIFEST"), "got: {error}");
        });
        std::fs::remove_file(manifest).unwrap();
    }

    #[test]
    fn test_expand_user() {
        let home = home_dir();
        assert_eq!(expand_user("~/x"), home.join("x"));
        assert_eq!(expand_user("~"), home);
        assert_eq!(expand_user("/abs/x"), PathBuf::from("/abs/x"));
    }

    /// tdd must be found under the developer's real home, never under the
    /// scratch home `td-common` mocks for tests.
    #[test]
    fn test_home_is_not_the_mocked_test_home() {
        assert_ne!(home_dir(), td_common::env::get_home_dir());
        assert!(!home_dir().starts_with(td_common::testdir::root().join("target")));
    }

    /// `$HOME` wins over the passwd entry, matching Python's `expanduser` — the
    /// fixtures of both languages have to find the same tdd.
    #[test]
    fn test_home_prefers_the_home_env_var() {
        temp_env::with_var(ENV_HOME, Some("/tmp/a-home"), || {
            assert_eq!(home_dir(), PathBuf::from("/tmp/a-home"));
            assert_eq!(expand_user("~/x"), PathBuf::from("/tmp/a-home/x"));
        });
        temp_env::with_var(ENV_HOME, Some(""), || {
            assert_eq!(home_dir(), homedir::my_home().unwrap().unwrap());
        });
    }

    #[test]
    fn test_exe_lives_in_the_tdd_venv() {
        temp_env::with_var(ENV_TDD_DIR, Some("/tmp/a-tdd"), || {
            let exe = exe();
            assert!(exe.starts_with("/tmp/a-tdd/tdd-venvs/tdd"), "got: {exe:?}");
        });
        temp_env::with_var_unset(ENV_TDD_DIR, || {
            assert!(exe().starts_with(home_dir().join(DEFAULT_TDD_DIR)));
        });
    }

    #[test]
    fn test_a_missing_tdd_says_how_to_install_it() {
        temp_env::with_var(ENV_TDD_DIR, Some("/nonexistent/tdd"), || {
            let err = names(Role::System).unwrap_err();
            assert!(err.contains("cargo make permit"), "got: {err}");
        });
    }

    /// Every field tdd prints, including the ones the fixtures do not read: those
    /// must be ignored, not break the parse.
    #[test]
    fn test_deserialize_a_domain() {
        let d = parse(
            r#"
            {
                "role": "domain",
                "generator": "tdx2",
                "system": "s3",
                "name": "landing",
                "uuid": "u-1",
                "started": "2026-01-01T00:00:00Z",
                "sys_name": "s30",
                "sys_uuid": "u-0",
                "external_name": "d_tucu_0_landing",
                "meta": {
                    "BUCKET": "b",
                    "PREFIX": "d_tucu_0_landing"
                },
                "env_refs": {},
                "model": "world",
                "data": "xs",
                "entities": {
                    "csv/city_1.csv": 42
                },
                "seeded": {},
                "seed": 3
            }
            "#,
        );
        assert_eq!(d.role, Role::Domain);
        assert_eq!(d.system, "s3");
        assert_eq!(d.name, "landing");
        assert_eq!(d.external_name, "d_tucu_0_landing");
        assert_eq!(d.meta["BUCKET"], "b");
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.entities["csv/city_1.csv"], 42);
    }

    #[test]
    fn test_deserialize_a_system() {
        let s = parse(
            r#"
            {
                "role": "system",
                "generator": "tdx2",
                "system": "s3",
                "name": "s30",
                "meta": {
                    "BUCKET": "b",
                    "SYS_PATH": "root"
                },
                "env_refs": {},
                "opt_literals": [],
                "sys_hostname": ""
            }
            "#,
        );
        assert_eq!(s.role, Role::System);
        assert_eq!(s.meta["SYS_PATH"], "root");
        assert!(s.entities.is_empty());
        assert_eq!(s.model, None);
    }
}
