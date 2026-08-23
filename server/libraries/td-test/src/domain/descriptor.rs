//
// Copyright 2026 Tabsdata Inc.
//

//! The persisted state for systems and domains, as written by `tdx-system` /
//! `tdx-domain`.
//!
//! Port of `tabsdatax.itest.tdksystems2.descriptor` (read side). [`load`]
//! returns a [`SystemInfo`] or [`DomainInfo`], chosen by the `role`
//! discriminator. `meta` stays an open-ended `KEY -> value` bag of
//! system-specific connection settings — the fixture dataclasses project it
//! onto typed fields.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use super::crypto;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "role")]
pub enum Info {
    #[serde(rename = "system")]
    System(SystemInfo),
    #[serde(rename = "domain")]
    Domain(DomainInfo),
}

#[derive(Debug, Clone, Deserialize)]
pub struct SystemInfo {
    /// Slug, e.g. `s3`.
    pub system: String,
    pub name: String,
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub started: String,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DomainInfo {
    pub system: String,
    pub name: String,
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub started: String,
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
    #[serde(default)]
    pub seed: i64,
}

impl Info {
    pub fn role(&self) -> super::Role {
        match self {
            Info::System(_) => super::Role::System,
            Info::Domain(_) => super::Role::Domain,
        }
    }

    pub fn system(&self) -> &str {
        match self {
            Info::System(i) => &i.system,
            Info::Domain(i) => &i.system,
        }
    }
}

/// Read a descriptor. A legacy `TDX2` one is an error naming the `upgrade`
/// command — tests read the store, they never rewrite it.
pub fn load(path: &Path) -> Result<Info, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read descriptor {}: {e}", path.display()))?;
    loads(&text)
}

pub fn loads(text: &str) -> Result<Info, String> {
    let json = crypto::decrypt(text)?;
    serde_json::from_slice(&json).map_err(|e| format!("malformed descriptor: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_domain() {
        let json = r#"{
            "role": "domain", "generator": "tdx2", "system": "s3", "name": "landing",
            "uuid": "u-1", "started": "2026-01-01T00:00:00Z",
            "sys_name": "s30", "sys_uuid": "u-0", "external_name": "d_tucu_0_landing",
            "meta": {"BUCKET": "b", "PREFIX": "d_tucu_0_landing"},
            "env_refs": {}, "model": "world", "data": "xs",
            "entities": {"csv/city_1.csv": 42}, "seed": 3
        }"#;
        let Info::Domain(d) = serde_json::from_str::<Info>(json).unwrap() else {
            panic!("expected a domain");
        };
        assert_eq!(d.system, "s3");
        assert_eq!(d.name, "landing");
        assert_eq!(d.external_name, "d_tucu_0_landing");
        assert_eq!(d.meta["BUCKET"], "b");
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.entities["csv/city_1.csv"], 42);
        assert_eq!(d.seed, 3);
    }

    #[test]
    fn test_deserialize_system() {
        let json = r#"{
            "role": "system", "generator": "tdx2", "system": "s3", "name": "s30",
            "uuid": "u-0", "started": "2026-01-01T00:00:00Z",
            "meta": {"BUCKET": "b", "SYS_PATH": "root"},
            "env_refs": {}, "opt_literals": [], "sys_hostname": ""
        }"#;
        let info = serde_json::from_str::<Info>(json).unwrap();
        assert_eq!(info.role(), super::super::Role::System);
        assert_eq!(info.system(), "s3");
        let Info::System(s) = info else {
            panic!("expected a system");
        };
        assert_eq!(s.name, "s30");
        assert_eq!(s.meta["SYS_PATH"], "root");
    }

    /// Descriptors are written by the Python tools and gain fields over time;
    /// unknown ones must not break an older Rust reader.
    #[test]
    fn test_unknown_fields_are_ignored() {
        let json = r#"{"role": "domain", "system": "s3", "name": "d",
                       "some_future_field": {"a": 1}}"#;
        let Info::Domain(d) = serde_json::from_str::<Info>(json).unwrap() else {
            panic!("expected a domain");
        };
        assert_eq!(d.name, "d");
        assert!(d.entities.is_empty());
        assert_eq!(d.model, None);
    }

    #[test]
    fn test_loads_rejects_non_descriptor() {
        assert!(loads("not a descriptor").unwrap_err().contains("PEM armor"));
    }

    const FIXTURE_SECRET: &str = "a-test-secret-long-enough";

    fn fixture(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("resources/test/domain")
            .join(name)
    }

    /// Cross-language guard: this descriptor was written by the Python
    /// `tdksystems2.crypto.encrypt` + `DomainInfo.model_dump_json`, so it fails
    /// the moment the two sides disagree on the KDF, the cipher, the armor, or
    /// the JSON shape. Regenerate with:
    ///
    /// ```text
    /// TD_BACKENDS_CRYPTO_KEY=a-test-secret-long-enough \
    ///   python -c "from tabsdatax.itest.tdksystems2 import crypto, descriptor; ..."
    /// ```
    fn assert_is_the_landing_domain(info: &Info) {
        let Info::Domain(d) = info else {
            panic!("expected a domain");
        };
        assert_eq!(d.system, "s3");
        assert_eq!(d.name, "landing");
        assert_eq!(d.sys_name, "s30");
        assert_eq!(d.external_name, "d_tucu_0_landing");
        assert_eq!(d.meta["BUCKET"], "a-bucket");
        assert_eq!(d.meta["BASE_PATH"], "root/d_tucu_0_landing");
        assert_eq!(d.meta["PREFIX"], "d_tucu_0_landing");
        assert_eq!(d.meta["REGION"], "us-east-1");
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.data, None);
        assert_eq!(d.entities["csv/city_1.csv"], 7);
    }

    #[test]
    fn test_reads_a_python_written_descriptor() {
        let path = fixture("python_written_s3_domain.pem");
        let before = std::fs::read_to_string(&path).unwrap();
        temp_env::with_var(crypto::ENV_CRYPTO_KEY, Some(FIXTURE_SECRET), || {
            assert_is_the_landing_domain(&load(&path).unwrap());
        });
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            before,
            "reading a current-format descriptor must not rewrite it"
        );
    }

    /// The same file must build the public fixture struct end to end.
    #[test]
    fn test_python_descriptor_builds_the_s3_domain() {
        use crate::domain::{Attrs, Backend, S3Domain};

        let path = fixture("python_written_s3_domain.pem");
        temp_env::with_var(crypto::ENV_CRYPTO_KEY, Some(FIXTURE_SECRET), || {
            let domain = S3Domain::build(&Attrs::of(&load(&path).unwrap())).unwrap();
            assert_eq!(domain.name, "landing");
            assert_eq!(domain.prefix, "d_tucu_0_landing");
            assert_eq!(domain.bucket, "a-bucket");
            assert_eq!(domain.access_key, "ak");
            assert_eq!(
                domain.url().unwrap().as_str(),
                "s3://a-bucket/root/d_tucu_0_landing"
            );
            assert_eq!(domain.staged(".csv"), vec![("csv/city_1.csv", 7)]);
        });
    }

    /// The legacy fixture is a real Python-written TDX2 descriptor. Reading it
    /// must fail with the instruction to run the upgrade commands, and must
    /// leave the file exactly as it found it — converting a store is the
    /// operator's explicit act, never a test's side effect.
    #[test]
    fn test_rejects_a_legacy_descriptor_and_names_the_upgrade() {
        let path = fixture("python_written_s3_domain_tdx2.pem");
        let before = std::fs::read_to_string(&path).unwrap();
        assert!(crypto::is_legacy(&before), "fixture must be TDX2");

        temp_env::with_var(crypto::ENV_CRYPTO_KEY, Some(FIXTURE_SECRET), || {
            let err = load(&path).unwrap_err();
            assert!(err.contains("tdx-system upgrade"), "got: {err}");
            assert!(err.contains("tdx-domain upgrade"), "got: {err}");
        });

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            before,
            "a rejected read must not touch the descriptor"
        );
    }
}
