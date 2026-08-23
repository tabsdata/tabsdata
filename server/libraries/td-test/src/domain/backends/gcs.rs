//
// Copyright 2026 Tabsdata Inc.
//

//! Fixture structs for the `gcs` backend — port of
//! `tabsdatax.itest.fixture.backends.gcs`.
//!
//! A tdx `gcs` system is a bucket plus a staging-root prefix (`SYS_PATH`); a
//! domain is one prefix under it (`BASE_PATH`, named `PREFIX`), holding the
//! staged files `create-data` wrote.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use url::Url;

use super::{as_prefix, join_prefix, store_of, url_of};

const SCHEME: &str = "gs";

crate::define_backend! {
    /// A `gcs` system: the bucket and the staging root every domain sits under.
    pub struct GcsSystem: system("gcs") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        bucket: String,
        service_account_key: String,
        /// S3-interop HMAC key, absent unless the system carries S3-compat
        /// credentials.
        hmac_access_key: Option<String>,
        hmac_secret_key: Option<String>,
    }
}

crate::define_backend! {
    /// A `gcs` domain: one prefix in the system's bucket.
    pub struct GcsDomain: domain("gcs") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        base_path: PathBuf,
        /// The domain's object-store prefix (== the entity container).
        prefix: String,
        bucket: String,
        service_account_key: String,
        /// S3-interop HMAC key, absent unless the system carries S3-compat
        /// credentials. Consumed by connectors that reach GCS over the S3
        /// protocol (ClickHouse / StarRocks staging) — see [`GcsDomain::s3_options`].
        hmac_access_key: Option<String>,
        hmac_secret_key: Option<String>,
        model: Option<String>,
        data: Option<String>,
        /// `{staged file path: record count}`, once loaded.
        entities: Option<BTreeMap<String, i64>>,
    }
}

/// The `object_store` config both a [`GcsSystem`] and a [`GcsDomain`] hand out.
///
/// The descriptor's service-account key is either inline JSON or a path to it —
/// the same either/or the Python backend resolves — and those are two different
/// `object_store` keys.
fn options(service_account_key: &str) -> HashMap<String, String> {
    let key = service_account_key.trim();
    let name = match key.starts_with('{') {
        true => "google_service_account_key",
        false => "google_service_account",
    };
    super::options([(name, key)])
}

/// The `aws_*` config for reaching this bucket over GCS's S3-interop endpoint,
/// or `None` when the descriptor carries no HMAC credentials.
fn s3_options(
    hmac_access_key: Option<&str>,
    hmac_secret_key: Option<&str>,
) -> Option<HashMap<String, String>> {
    let (access_key, secret_key) = (hmac_access_key?, hmac_secret_key?);
    if access_key.is_empty() || secret_key.is_empty() {
        return None;
    }
    Some(super::options([
        ("aws_access_key_id", access_key),
        ("aws_secret_access_key", secret_key),
        ("aws_endpoint", "https://storage.googleapis.com"),
    ]))
}

impl GcsSystem {
    /// `google_*` config for `object_store` / polars `CloudOptions`.
    pub fn options(&self) -> HashMap<String, String> {
        options(&self.service_account_key)
    }

    /// See [`GcsDomain::s3_options`].
    pub fn s3_options(&self) -> Option<HashMap<String, String>> {
        s3_options(
            self.hmac_access_key.as_deref(),
            self.hmac_secret_key.as_deref(),
        )
    }

    /// `gs://<bucket>/<sys_path>` — the staging root, not a domain.
    pub fn url(&self) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.sys_path_prefix())
    }

    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, String> {
        store_of(&self.url()?, &self.options())
    }

    /// `sys_path` as a slash-separated object-store prefix, no leading or
    /// trailing separator.
    pub fn sys_path_prefix(&self) -> String {
        as_prefix(&self.sys_path)
    }

    /// A unique child prefix for one test to write into:
    /// `<sys_path>/<area>/<uuid4>`. See [`run_prefix`](crate::domain::run_prefix).
    pub fn run_prefix(&self, area: &str) -> String {
        crate::domain::run_prefix(self.sys_path_prefix(), area)
    }

    /// [`run_prefix`](Self::run_prefix) as a url —
    /// `gs://<bucket>/<sys_path>/<area>/<uuid4>`.
    pub fn run_url(&self, area: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.run_prefix(area))
    }
}

impl GcsDomain {
    /// `google_*` config for `object_store` / polars `CloudOptions`.
    pub fn options(&self) -> HashMap<String, String> {
        options(&self.service_account_key)
    }

    /// `aws_*` config for reaching the same bucket over GCS's S3-interop
    /// endpoint, or `None` when the descriptor carries no HMAC credentials.
    pub fn s3_options(&self) -> Option<HashMap<String, String>> {
        s3_options(
            self.hmac_access_key.as_deref(),
            self.hmac_secret_key.as_deref(),
        )
    }

    /// `gs://<bucket>/<base_path>` — the domain's own prefix.
    pub fn url(&self) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.base_path_prefix())
    }

    /// `gs://<bucket>/<base_path>/<child>`.
    pub fn child_url(&self, child: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.child_prefix(child))
    }

    /// `<base_path>/<child>` — the store key for a path
    /// [`staged`](Self::staged) returned, since
    /// [`object_store`](Self::object_store) is rooted at the bucket.
    pub fn child_prefix(&self, child: &str) -> String {
        join_prefix(&self.base_path_prefix(), child)
    }

    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, String> {
        store_of(&self.url()?, &self.options())
    }

    /// `base_path` as a slash-separated object-store prefix, no leading or
    /// trailing separator.
    pub fn base_path_prefix(&self) -> String {
        as_prefix(&self.base_path)
    }

    /// A unique child prefix for one test to write into:
    /// `<base_path>/<area>/<uuid4>`. See [`run_prefix`](crate::domain::run_prefix).
    pub fn run_prefix(&self, area: &str) -> String {
        crate::domain::run_prefix(self.base_path_prefix(), area)
    }

    /// [`run_prefix`](Self::run_prefix) as a url —
    /// `gs://<bucket>/<base_path>/<area>/<uuid4>`.
    pub fn run_url(&self, area: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.run_prefix(area))
    }

    /// The staged entity files matching `suffix`, e.g. `.csv`, with their row
    /// counts. Empty until `tdx-domain create-data` has run.
    pub fn staged(&self, suffix: &str) -> Vec<(&str, i64)> {
        self.entities
            .iter()
            .flatten()
            .filter(|(path, _)| path.ends_with(suffix))
            .map(|(path, rows)| (path.as_str(), *rows))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::descriptor::{DomainInfo, Info, SystemInfo};
    use crate::domain::{Attrs, Backend, Role};

    const SERVICE_ACCOUNT_JSON: &str = r#"{"type": "service_account"}"#;

    fn system_info(meta: BTreeMap<String, String>) -> Info {
        Info::System(SystemInfo {
            system: "gcs".into(),
            name: "gcs".into(),
            uuid: String::new(),
            started: String::new(),
            meta,
        })
    }

    fn domain_meta() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "file".to_string()),
            ("SYS_PATH".to_string(), "root".to_string()),
            ("BASE_PATH".to_string(), "root/d_tucu_0_landing".to_string()),
            ("PREFIX".to_string(), "d_tucu_0_landing".to_string()),
            ("BUCKET".to_string(), "a-bucket".to_string()),
            (
                "SERVICE_ACCOUNT_KEY".to_string(),
                SERVICE_ACCOUNT_JSON.to_string(),
            ),
            ("HMAC_ACCESS_KEY".to_string(), "hak".to_string()),
            ("HMAC_SECRET_KEY".to_string(), "hsk".to_string()),
        ])
    }

    fn domain_info(meta: BTreeMap<String, String>) -> Info {
        Info::Domain(DomainInfo {
            system: "gcs".into(),
            name: "landing".into(),
            uuid: String::new(),
            started: String::new(),
            sys_name: "gcs".into(),
            external_name: "d_tucu_0_landing".into(),
            meta,
            model: Some("world".into()),
            data: None,
            entities: BTreeMap::from([
                ("csv/city_1.csv".to_string(), 7),
                ("parquet/city_1.parquet".to_string(), 7),
            ]),
            seed: 0,
        })
    }

    fn domain() -> GcsDomain {
        GcsDomain::build(&Attrs::of(&domain_info(domain_meta()))).unwrap()
    }

    #[test]
    fn test_roles_and_system_types() {
        assert_eq!(GcsSystem::SYSTEM_TYPE, "gcs");
        assert_eq!(GcsSystem::ROLE, Role::System);
        assert_eq!(GcsDomain::SYSTEM_TYPE, "gcs");
        assert_eq!(GcsDomain::ROLE, Role::Domain);
    }

    #[test]
    fn test_build_domain_from_descriptor() {
        let d = domain();
        assert_eq!(d.name, "landing");
        assert_eq!(d.system_kind, "file");
        assert_eq!(d.sys_path, PathBuf::from("root"));
        assert_eq!(d.base_path, PathBuf::from("root/d_tucu_0_landing"));
        assert_eq!(d.prefix, "d_tucu_0_landing");
        assert_eq!(d.bucket, "a-bucket");
        assert_eq!(d.service_account_key, SERVICE_ACCOUNT_JSON);
        assert_eq!(d.hmac_access_key.as_deref(), Some("hak"));
        assert_eq!(d.hmac_secret_key.as_deref(), Some("hsk"));
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.data, None);
        assert_eq!(d.entities.as_ref().unwrap()["csv/city_1.csv"], 7);
    }

    #[test]
    fn test_build_system_from_descriptor() {
        let meta = BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "file".to_string()),
            ("SYS_PATH".to_string(), "root".to_string()),
            ("BUCKET".to_string(), "a-bucket".to_string()),
            (
                "SERVICE_ACCOUNT_KEY".to_string(),
                SERVICE_ACCOUNT_JSON.to_string(),
            ),
        ]);
        let s = GcsSystem::build(&Attrs::of(&system_info(meta))).unwrap();
        assert_eq!(s.name, "gcs");
        assert_eq!(s.sys_path, PathBuf::from("root"));
        assert_eq!(s.url().unwrap().as_str(), "gs://a-bucket/root");
    }

    #[test]
    fn test_build_reports_every_missing_key() {
        let mut meta = domain_meta();
        meta.remove("BUCKET");
        meta.remove("SERVICE_ACCOUNT_KEY");
        let err = GcsDomain::build(&Attrs::of(&domain_info(meta))).unwrap_err();
        assert!(err.contains("BUCKET"), "got: {err}");
        assert!(err.contains("SERVICE_ACCOUNT_KEY"), "got: {err}");
    }

    /// The HMAC keys are optional: a system provisioned without S3-interop
    /// credentials still builds.
    #[test]
    fn test_hmac_keys_are_optional() {
        let mut meta = domain_meta();
        meta.remove("HMAC_ACCESS_KEY");
        meta.remove("HMAC_SECRET_KEY");
        let d = GcsDomain::build(&Attrs::of(&domain_info(meta))).unwrap();
        assert_eq!(d.hmac_access_key, None);
        assert_eq!(d.hmac_secret_key, None);
        assert_eq!(d.s3_options(), None);
    }

    #[test]
    fn test_urls() {
        let d = domain();
        assert_eq!(
            d.url().unwrap().as_str(),
            "gs://a-bucket/root/d_tucu_0_landing"
        );
        assert_eq!(
            d.child_url("csv/city_1.csv").unwrap().as_str(),
            "gs://a-bucket/root/d_tucu_0_landing/csv/city_1.csv"
        );
        assert_eq!(
            d.child_url("/csv/").unwrap().as_str(),
            "gs://a-bucket/root/d_tucu_0_landing/csv"
        );
    }

    /// An inline-JSON key and a path to one are two different `object_store`
    /// keys, and the descriptor may carry either.
    #[test]
    fn test_options_route_the_service_account_key_by_shape() {
        assert_eq!(
            domain().options()["google_service_account_key"],
            SERVICE_ACCOUNT_JSON
        );

        let mut meta = domain_meta();
        meta.insert(
            "SERVICE_ACCOUNT_KEY".to_string(),
            "/keys/sa.json".to_string(),
        );
        let by_path = GcsDomain::build(&Attrs::of(&domain_info(meta))).unwrap();
        let opts = by_path.options();
        assert_eq!(opts["google_service_account"], "/keys/sa.json");
        assert!(!opts.contains_key("google_service_account_key"));
    }

    #[test]
    fn test_options_drop_a_blank_service_account_key() {
        let mut meta = domain_meta();
        meta.insert("SERVICE_ACCOUNT_KEY".to_string(), String::new());
        let anon = GcsDomain::build(&Attrs::of(&domain_info(meta))).unwrap();
        assert!(anon.options().is_empty());
    }

    #[test]
    fn test_s3_options() {
        let opts = domain().s3_options().unwrap();
        assert_eq!(opts["aws_access_key_id"], "hak");
        assert_eq!(opts["aws_secret_access_key"], "hsk");
        assert_eq!(opts["aws_endpoint"], "https://storage.googleapis.com");

        let mut meta = domain_meta();
        meta.insert("HMAC_SECRET_KEY".to_string(), String::new());
        let blank = GcsDomain::build(&Attrs::of(&domain_info(meta))).unwrap();
        assert_eq!(blank.s3_options(), None);
    }

    /// The store is rooted at the bucket, so a staged path reaches it only
    /// through [`child_prefix`].
    #[test]
    fn test_child_prefix_addresses_the_store() {
        let d = domain();
        assert_eq!(
            d.child_prefix("csv/city_1.csv"),
            "root/d_tucu_0_landing/csv/city_1.csv"
        );
        assert_eq!(d.child_prefix("/csv/"), "root/d_tucu_0_landing/csv");
    }

    #[test]
    fn test_run_prefix_is_unique_under_base_path() {
        let d = domain();
        let a = d.run_prefix("td-integ");
        let b = d.run_prefix("td-integ");
        assert_ne!(a, b);
        for prefix in [&a, &b] {
            assert!(
                prefix.starts_with("root/d_tucu_0_landing/td-integ/"),
                "got: {prefix}"
            );
        }
    }

    #[test]
    fn test_run_url_is_the_run_prefix_as_a_url() {
        let d = domain();
        let url = d.run_url("td-integ").unwrap();
        assert!(
            url.as_str()
                .starts_with("gs://a-bucket/root/d_tucu_0_landing/td-integ/"),
            "got: {url}"
        );
    }

    #[test]
    fn test_staged() {
        let d = domain();
        assert_eq!(d.staged(".csv"), vec![("csv/city_1.csv", 7)]);
        assert_eq!(d.staged(".ndjson"), vec![]);
    }

    #[test]
    fn test_staged_without_create_data() {
        let Info::Domain(mut info) = domain_info(domain_meta()) else {
            unreachable!()
        };
        info.entities.clear();
        let d = GcsDomain::build(&Attrs::of(&Info::Domain(info))).unwrap();
        assert_eq!(d.entities, None);
        assert_eq!(d.staged(".csv"), vec![]);
    }
}
