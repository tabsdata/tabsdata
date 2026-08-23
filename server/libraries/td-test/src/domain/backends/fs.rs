//
// Copyright 2026 Tabsdata Inc.
//

//! Fixture structs for the `fs` (local filesystem) backend — port of
//! `tabsdatax.itest.fixture.backends.fs`.
//!
//! A tdx `fs` system is a real directory (the staging root, `SYS_PATH`); a
//! domain is one subdirectory under it (`BASE_PATH`, named `FOLDER`), holding
//! the staged files `create-data` wrote.
//!
//! Unlike the object-store backends there is no container above the root, so
//! these deal in `PathBuf`s and their [`object_store`](FsDomain::object_store)
//! is rooted at the fixture's own directory — store paths are relative to it,
//! where an `s3` store's are the full keys from the bucket.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use url::Url;

crate::define_backend! {
    /// An `fs` system: the directory every domain sits under.
    pub struct FsSystem: system("fs") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
    }
}

crate::define_backend! {
    /// An `fs` domain: one directory under the system's root.
    pub struct FsDomain: domain("fs") {
        name: String,
        system_kind: String,
        sys_path: PathBuf,
        base_path: PathBuf,
        /// The domain's directory name (== the entity container).
        folder: String,
        model: Option<String>,
        data: Option<String>,
        /// `{staged file path: record count}`, once loaded.
        entities: Option<BTreeMap<String, i64>>,
    }
}

fn url_of(path: &Path) -> Result<Url, String> {
    Url::from_file_path(path)
        .map_err(|_| format!("cannot build a file url from '{}'", path.display()))
}

fn store_of(path: &Path) -> Result<Arc<dyn ObjectStore>, String> {
    LocalFileSystem::new_with_prefix(path)
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|e| format!("cannot open the local store at {}: {e}", path.display()))
}

impl FsSystem {
    /// The staging root, not a domain.
    pub fn path(&self) -> &Path {
        &self.sys_path
    }

    /// `file://<sys_path>`.
    pub fn url(&self) -> Result<Url, String> {
        url_of(&self.sys_path)
    }

    /// A store rooted at [`path`](Self::path), so its paths are relative to the
    /// staging root.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, String> {
        store_of(&self.sys_path)
    }

    /// A unique directory for one test to write into:
    /// `<sys_path>/<area>/<uuid4>`. Not created — that is the test's to do.
    /// See [`run_prefix`](crate::domain::run_prefix).
    pub fn run_path(&self, area: &str) -> PathBuf {
        self.sys_path.join(crate::domain::run_prefix("", area))
    }
}

impl FsDomain {
    /// The domain's own directory.
    pub fn path(&self) -> &Path {
        &self.base_path
    }

    /// `<base_path>/<child>`, e.g. for a path [`staged`](Self::staged) returned.
    pub fn child_path(&self, child: &str) -> PathBuf {
        self.base_path.join(child.trim_matches('/'))
    }

    /// `file://<base_path>`.
    pub fn url(&self) -> Result<Url, String> {
        url_of(&self.base_path)
    }

    /// `file://<base_path>/<child>`.
    pub fn child_url(&self, child: &str) -> Result<Url, String> {
        url_of(&self.child_path(child))
    }

    /// A store rooted at [`path`](Self::path), so its paths are relative to the
    /// domain — the same relative paths [`staged`](Self::staged) returns.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, String> {
        store_of(&self.base_path)
    }

    /// A unique directory for one test to write into:
    /// `<base_path>/<area>/<uuid4>`. Not created — that is the test's to do.
    /// See [`run_prefix`](crate::domain::run_prefix).
    pub fn run_path(&self, area: &str) -> PathBuf {
        self.base_path.join(crate::domain::run_prefix("", area))
    }

    /// The staged entity files matching `suffix`, e.g. `.csv`, with their row
    /// counts. Paths are relative to [`path`](Self::path); empty until
    /// `tdx-domain create-data` has run.
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
    use object_store::ObjectStoreExt;

    const ROOT: &str = "/srv/tdx/backends/fs";
    const BASE: &str = "/srv/tdx/backends/fs/d_tucu_0_landing";

    fn system_info(meta: BTreeMap<String, String>) -> Info {
        Info::System(SystemInfo {
            system: "fs".into(),
            name: "fs".into(),
            uuid: String::new(),
            started: String::new(),
            meta,
        })
    }

    fn domain_meta() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "file".to_string()),
            ("SYS_PATH".to_string(), ROOT.to_string()),
            ("BASE_PATH".to_string(), BASE.to_string()),
            ("FOLDER".to_string(), "d_tucu_0_landing".to_string()),
        ])
    }

    fn domain_info(meta: BTreeMap<String, String>) -> Info {
        Info::Domain(DomainInfo {
            system: "fs".into(),
            name: "landing".into(),
            uuid: String::new(),
            started: String::new(),
            sys_name: "fs".into(),
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

    fn domain() -> FsDomain {
        FsDomain::build(&Attrs::of(&domain_info(domain_meta()))).unwrap()
    }

    #[test]
    fn test_roles_and_system_types() {
        assert_eq!(FsSystem::SYSTEM_TYPE, "fs");
        assert_eq!(FsSystem::ROLE, Role::System);
        assert_eq!(FsDomain::SYSTEM_TYPE, "fs");
        assert_eq!(FsDomain::ROLE, Role::Domain);
    }

    #[test]
    fn test_build_domain_from_descriptor() {
        let d = domain();
        assert_eq!(d.name, "landing");
        assert_eq!(d.system_kind, "file");
        assert_eq!(d.sys_path, PathBuf::from(ROOT));
        assert_eq!(d.base_path, PathBuf::from(BASE));
        assert_eq!(d.folder, "d_tucu_0_landing");
        assert_eq!(d.model.as_deref(), Some("world"));
        assert_eq!(d.data, None);
        assert_eq!(d.entities.as_ref().unwrap()["csv/city_1.csv"], 7);
    }

    #[test]
    fn test_build_system_from_descriptor() {
        let meta = BTreeMap::from([
            ("SYSTEM_KIND".to_string(), "file".to_string()),
            ("SYS_PATH".to_string(), ROOT.to_string()),
        ]);
        let s = FsSystem::build(&Attrs::of(&system_info(meta))).unwrap();
        assert_eq!(s.name, "fs");
        assert_eq!(s.path(), Path::new(ROOT));
    }

    #[test]
    fn test_build_reports_every_missing_key() {
        let mut meta = domain_meta();
        meta.remove("BASE_PATH");
        meta.remove("FOLDER");
        let err = FsDomain::build(&Attrs::of(&domain_info(meta))).unwrap_err();
        assert!(err.contains("BASE_PATH"), "got: {err}");
        assert!(err.contains("FOLDER"), "got: {err}");
    }

    #[test]
    fn test_paths() {
        let d = domain();
        assert_eq!(d.path(), Path::new(BASE));
        assert_eq!(
            d.child_path("csv/city_1.csv"),
            PathBuf::from(format!("{BASE}/csv/city_1.csv"))
        );
        assert_eq!(d.child_path("/csv/"), PathBuf::from(format!("{BASE}/csv")));
    }

    #[cfg(unix)]
    #[test]
    fn test_urls() {
        let d = domain();
        assert_eq!(d.url().unwrap().as_str(), format!("file://{BASE}"));
        assert_eq!(
            d.child_url("csv/city_1.csv").unwrap().as_str(),
            format!("file://{BASE}/csv/city_1.csv")
        );
    }

    /// The store is rooted at the domain, so a staged path addresses into it
    /// as-is.
    #[tokio::test]
    async fn test_object_store_is_rooted_at_the_domain() {
        let base =
            std::env::temp_dir().join(format!("td-test-fs-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(base.join("csv")).unwrap();
        std::fs::write(base.join("csv/city_1.csv"), b"id\n1\n").unwrap();

        let mut meta = domain_meta();
        meta.insert("BASE_PATH".to_string(), base.to_str().unwrap().to_string());
        let d = FsDomain::build(&Attrs::of(&domain_info(meta))).unwrap();

        let store = d.object_store().unwrap();
        let (path, _) = d.staged(".csv")[0];
        let bytes = store
            .get(&object_store::path::Path::from(path))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"id\n1\n");

        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn test_object_store_names_an_absent_root() {
        let d = domain();
        let err = d.object_store().unwrap_err();
        assert!(err.contains(BASE), "got: {err}");
    }

    #[test]
    fn test_run_path_is_unique_under_base_path() {
        let d = domain();
        let a = d.run_path("td-integ");
        let b = d.run_path("td-integ");
        assert_ne!(a, b);
        for path in [&a, &b] {
            assert!(
                path.starts_with(format!("{BASE}/td-integ")),
                "got: {}",
                path.display()
            );
        }
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
        let d = FsDomain::build(&Attrs::of(&Info::Domain(info))).unwrap();
        assert_eq!(d.entities, None);
        assert_eq!(d.staged(".csv"), vec![]);
    }
}
