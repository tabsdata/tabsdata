//
// Copyright 2026 Tabsdata Inc.
//

//! Fixture structs for the `azureblob` backend — port of
//! `tabsdatax.itest.fixture.backends.azureblob`.
//!
//! A tdx `azureblob` system is a blob container plus a staging-root prefix
//! (`SYS_PATH`); a domain is one prefix under it (`BASE_PATH`, named `PREFIX`),
//! holding the staged files `create-data` wrote.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use url::Url;

use super::{as_prefix, join_prefix, store_of, url_of};

const SCHEME: &str = "az";

crate::define_backend! {
    /// An `azureblob` system: the container and the staging root every domain
    /// sits under.
    pub struct AzureblobSystem: system("azureblob") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        container: String,
        account_name: String,
        account_key: String,
    }
}

crate::define_backend! {
    /// An `azureblob` domain: one prefix in the system's container.
    pub struct AzureblobDomain: domain("azureblob") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        base_path: PathBuf,
        /// The domain's object-store prefix (== the entity container).
        prefix: String,
        container: String,
        account_name: String,
        account_key: String,
        model: Option<String>,
        data: Option<String>,
        /// `{staged file path: record count}`, once loaded.
        entities: Option<BTreeMap<String, i64>>,
    }
}

/// The `object_store` config both an [`AzureblobSystem`] and an
/// [`AzureblobDomain`] hand out.
fn options(account_name: &str, account_key: &str) -> HashMap<String, String> {
    super::options([
        ("azure_storage_account_name", account_name),
        ("azure_storage_account_key", account_key),
    ])
}

impl AzureblobSystem {
    /// `azure_*` config for `object_store` / polars `CloudOptions`.
    pub fn options(&self) -> HashMap<String, String> {
        options(&self.account_name, &self.account_key)
    }

    /// `az://<container>/<sys_path>` — the staging root, not a domain.
    pub fn url(&self) -> Result<Url, String> {
        url_of(SCHEME, &self.container, &self.sys_path_prefix())
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
    /// `az://<container>/<sys_path>/<area>/<uuid4>`.
    pub fn run_url(&self, area: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.container, &self.run_prefix(area))
    }
}

impl AzureblobDomain {
    /// `azure_*` config for `object_store` / polars `CloudOptions`.
    pub fn options(&self) -> HashMap<String, String> {
        options(&self.account_name, &self.account_key)
    }

    /// `az://<container>/<base_path>` — the domain's own prefix.
    pub fn url(&self) -> Result<Url, String> {
        url_of(SCHEME, &self.container, &self.base_path_prefix())
    }

    /// `az://<container>/<base_path>/<child>`.
    pub fn child_url(&self, child: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.container, &self.child_prefix(child))
    }

    /// `<base_path>/<child>` — the store key for a path
    /// [`staged`](Self::staged) returned, since
    /// [`object_store`](Self::object_store) is rooted at the container.
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
    /// `az://<container>/<base_path>/<area>/<uuid4>`.
    pub fn run_url(&self, area: &str) -> Result<Url, String> {
        url_of(SCHEME, &self.container, &self.run_prefix(area))
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

    /// `object_store` base64-decodes the shared key, so a fixture value that is
    /// not base64 fails to build a store at all.
    const ACCOUNT_KEY: &str = "YWNjb3VudC1rZXk=";

    fn system_info(meta: BTreeMap<String, String>) -> Info {
        Info::System(SystemInfo {
            system: "azureblob".into(),
            name: "azureblob".into(),
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
            ("CONTAINER".to_string(), "a-container".to_string()),
            ("ACCOUNT_NAME".to_string(), "an".to_string()),
            ("ACCOUNT_KEY".to_string(), ACCOUNT_KEY.to_string()),
        ])
    }

    fn domain_info(meta: BTreeMap<String, String>) -> Info {
        Info::Domain(DomainInfo {
            system: "azureblob".into(),
            name: "landing".into(),
            uuid: String::new(),
            started: String::new(),
            sys_name: "azureblob".into(),
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

    fn domain() -> AzureblobDomain {
        AzureblobDomain::build(&Attrs::of(&domain_info(domain_meta()))).unwrap()
    }

    #[test]
    fn test_roles_and_system_types() {
        assert_eq!(AzureblobSystem::SYSTEM_TYPE, "azureblob");
        assert_eq!(AzureblobSystem::ROLE, Role::System);
        assert_eq!(AzureblobDomain::SYSTEM_TYPE, "azureblob");
        assert_eq!(AzureblobDomain::ROLE, Role::Domain);
    }

    #[test]
    fn test_build_domain_from_descriptor() {
        let d = domain();
        assert_eq!(d.name, "landing");
        assert_eq!(d.system_kind, "file");
        assert_eq!(d.sys_path, PathBuf::from("root"));
        assert_eq!(d.base_path, PathBuf::from("root/d_tucu_0_landing"));
        assert_eq!(d.prefix, "d_tucu_0_landing");
        assert_eq!(d.container, "a-container");
        assert_eq!(d.account_name, "an");
        assert_eq!(d.account_key, ACCOUNT_KEY);
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.data, None);
        assert_eq!(d.entities.as_ref().unwrap()["csv/city_1.csv"], 7);
    }

    #[test]
    fn test_build_system_from_descriptor() {
        let meta = BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "file".to_string()),
            ("SYS_PATH".to_string(), "root".to_string()),
            ("CONTAINER".to_string(), "a-container".to_string()),
            ("ACCOUNT_NAME".to_string(), "an".to_string()),
            ("ACCOUNT_KEY".to_string(), ACCOUNT_KEY.to_string()),
        ]);
        let s = AzureblobSystem::build(&Attrs::of(&system_info(meta))).unwrap();
        assert_eq!(s.name, "azureblob");
        assert_eq!(s.sys_path, PathBuf::from("root"));
        assert_eq!(s.url().unwrap().as_str(), "az://a-container/root");
    }

    #[test]
    fn test_build_reports_every_missing_key() {
        let mut meta = domain_meta();
        meta.remove("CONTAINER");
        meta.remove("ACCOUNT_NAME");
        let err = AzureblobDomain::build(&Attrs::of(&domain_info(meta))).unwrap_err();
        assert!(err.contains("CONTAINER"), "got: {err}");
        assert!(err.contains("ACCOUNT_NAME"), "got: {err}");
    }

    #[test]
    fn test_urls() {
        let d = domain();
        assert_eq!(
            d.url().unwrap().as_str(),
            "az://a-container/root/d_tucu_0_landing"
        );
        assert_eq!(
            d.child_url("csv/city_1.csv").unwrap().as_str(),
            "az://a-container/root/d_tucu_0_landing/csv/city_1.csv"
        );
        assert_eq!(
            d.child_url("/csv/").unwrap().as_str(),
            "az://a-container/root/d_tucu_0_landing/csv"
        );
    }

    /// `object_store` reads the container off the `az://` host, so the store a
    /// fixture hands out is rooted at the container and the prefix helpers
    /// address into it — same shape as `s3`.
    #[test]
    fn test_object_store_is_rooted_at_the_container() {
        let d = domain();
        d.object_store().unwrap();
        assert_eq!(d.base_path_prefix(), "root/d_tucu_0_landing");
    }

    #[test]
    fn test_options_drop_blank_credentials() {
        let d = domain();
        let opts = d.options();
        assert_eq!(opts["azure_storage_account_name"], "an");
        assert_eq!(opts["azure_storage_account_key"], ACCOUNT_KEY);

        let mut meta = domain_meta();
        meta.insert("ACCOUNT_KEY".to_string(), String::new());
        let anon = AzureblobDomain::build(&Attrs::of(&domain_info(meta))).unwrap();
        let opts = anon.options();
        assert!(!opts.contains_key("azure_storage_account_key"));
        assert_eq!(opts["azure_storage_account_name"], "an");
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
                .starts_with("az://a-container/root/d_tucu_0_landing/td-integ/"),
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
        let d = AzureblobDomain::build(&Attrs::of(&Info::Domain(info))).unwrap();
        assert_eq!(d.entities, None);
        assert_eq!(d.staged(".csv"), vec![]);
    }
}
