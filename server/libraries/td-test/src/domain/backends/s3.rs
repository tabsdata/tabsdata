//
// Copyright 2026 Tabsdata Inc.
//

//! Fixture structs for the `s3` backend — port of
//! `tabsdatax.itest.fixture.backends.s3`.
//!
//! A tdx `s3` system is a bucket plus a staging-root prefix (`SYS_PATH`); a
//! domain is one prefix under it (`BASE_PATH`, named `PREFIX`), holding the
//! staged files `create-data` wrote.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use url::Url;

use super::{as_prefix, join_prefix, store_of, url_of};

const SCHEME: &str = "s3";

crate::define_backend! {
    /// An `s3` system: the bucket and the staging root every domain sits under.
    pub struct S3System: system("s3") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    }
}

crate::define_backend! {
    /// An `s3` domain: one prefix in the system's bucket.
    pub struct S3Domain: domain("s3") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        base_path: PathBuf,
        /// The domain's object-store prefix (== the entity container).
        prefix: String,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
        model: Option<String>,
        data: Option<String>,
        /// `{staged file path: record count}`, once loaded.
        entities: Option<BTreeMap<String, i64>>,
    }
}

/// The `object_store` config both an [`S3System`] and an [`S3Domain`] hand out.
fn options(region: &str, access_key: &str, secret_key: &str) -> HashMap<String, String> {
    super::options([
        ("aws_region", region),
        ("aws_access_key_id", access_key),
        ("aws_secret_access_key", secret_key),
    ])
}

impl S3System {
    /// `aws_*` config for `object_store` / polars `CloudOptions`.
    pub fn options(&self) -> HashMap<String, String> {
        options(&self.region, &self.access_key, &self.secret_key)
    }

    /// `s3://<bucket>/<sys_path>` — the staging root, not a domain.
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
    /// `s3://<bucket>/<sys_path>/<area>/<uuid4>`.
    pub fn run_url(&self, area: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.run_prefix(area))
    }
}

impl S3Domain {
    /// `aws_*` config for `object_store` / polars `CloudOptions`.
    pub fn options(&self) -> HashMap<String, String> {
        options(&self.region, &self.access_key, &self.secret_key)
    }

    /// `s3://<bucket>/<base_path>` — the domain's own prefix.
    pub fn url(&self) -> Result<Url, String> {
        url_of(SCHEME, &self.bucket, &self.base_path_prefix())
    }

    /// `s3://<bucket>/<base_path>/<child>`.
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
    /// `s3://<bucket>/<base_path>/<area>/<uuid4>`.
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

    fn system_info(meta: BTreeMap<String, String>) -> Info {
        Info::System(SystemInfo {
            system: "s3".into(),
            name: "s30".into(),
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
            ("REGION".to_string(), "us-east-1".to_string()),
            ("ACCESS_KEY".to_string(), "ak".to_string()),
            ("SECRET_KEY".to_string(), "sk".to_string()),
        ])
    }

    fn domain_info(meta: BTreeMap<String, String>) -> Info {
        Info::Domain(DomainInfo {
            system: "s3".into(),
            name: "landing".into(),
            uuid: String::new(),
            started: String::new(),
            sys_name: "s30".into(),
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

    fn domain() -> S3Domain {
        S3Domain::build(&Attrs::of(&domain_info(domain_meta()))).unwrap()
    }

    #[test]
    fn test_roles_and_system_types() {
        assert_eq!(S3System::SYSTEM_TYPE, "s3");
        assert_eq!(S3System::ROLE, Role::System);
        assert_eq!(S3Domain::SYSTEM_TYPE, "s3");
        assert_eq!(S3Domain::ROLE, Role::Domain);
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
        assert_eq!(d.region, "us-east-1");
        assert_eq!(d.access_key, "ak");
        assert_eq!(d.secret_key, "sk");
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
            ("REGION".to_string(), "us-east-1".to_string()),
            ("ACCESS_KEY".to_string(), "ak".to_string()),
            ("SECRET_KEY".to_string(), "sk".to_string()),
        ]);
        let s = S3System::build(&Attrs::of(&system_info(meta))).unwrap();
        assert_eq!(s.name, "s30");
        assert_eq!(s.sys_path, PathBuf::from("root"));
        assert_eq!(s.url().unwrap().as_str(), "s3://a-bucket/root");
    }

    #[test]
    fn test_build_reports_every_missing_key() {
        let mut meta = domain_meta();
        meta.remove("BUCKET");
        meta.remove("REGION");
        let err = S3Domain::build(&Attrs::of(&domain_info(meta))).unwrap_err();
        assert!(err.contains("BUCKET"), "got: {err}");
        assert!(err.contains("REGION"), "got: {err}");
    }

    #[test]
    fn test_urls() {
        let d = domain();
        assert_eq!(
            d.url().unwrap().as_str(),
            "s3://a-bucket/root/d_tucu_0_landing"
        );
        assert_eq!(
            d.child_url("csv/city_1.csv").unwrap().as_str(),
            "s3://a-bucket/root/d_tucu_0_landing/csv/city_1.csv"
        );
        assert_eq!(
            d.child_url("/csv/").unwrap().as_str(),
            "s3://a-bucket/root/d_tucu_0_landing/csv"
        );
    }

    #[test]
    fn test_options_drop_blank_credentials() {
        let d = domain();
        let opts = d.options();
        assert_eq!(opts["aws_region"], "us-east-1");
        assert_eq!(opts["aws_access_key_id"], "ak");
        assert_eq!(opts["aws_secret_access_key"], "sk");

        let mut meta = domain_meta();
        meta.insert("ACCESS_KEY".to_string(), String::new());
        meta.insert("SECRET_KEY".to_string(), String::new());
        let anon = S3Domain::build(&Attrs::of(&domain_info(meta))).unwrap();
        let opts = anon.options();
        assert!(!opts.contains_key("aws_access_key_id"));
        assert!(!opts.contains_key("aws_secret_access_key"));
        assert_eq!(opts["aws_region"], "us-east-1");
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
                .starts_with("s3://a-bucket/root/d_tucu_0_landing/td-integ/"),
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
        let d = S3Domain::build(&Attrs::of(&Info::Domain(info))).unwrap();
        assert_eq!(d.entities, None);
        assert_eq!(d.staged(".csv"), vec![]);
    }
}
