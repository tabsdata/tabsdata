//
// Copyright 2024 Tabs Data Inc.
//

use super::{Result, SPath, StorageError};
use bytes::Bytes;
use derive_builder::Builder;
use futures_util::stream::BoxStream;
use getset::Getters;
use object_store::path::{Path, PathPart};
use object_store::{ObjectStore, PutPayload};
#[cfg(target_os = "windows")]
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use td_common::absolute_path::AbsolutePath;
use tracing::debug;
use url::Url;

/// Definition of a mount.
#[derive(Debug, Serialize, Deserialize, Builder, Getters)]
#[builder(
    setter(into, strip_option),
    build_fn(validate = "Self::validate", error = "StorageError")
)]
#[getset(get = "pub")]
pub struct MountDef {
    /// Path, in the storage, where the mount is located.
    mount_path: String,

    #[builder(setter(custom))]
    /// External URI that is backing the storage of the mount.
    uri: String,

    #[builder(default)]
    /// Configurations for the mount. This is [`uri`] scheme specific.
    ///
    /// AWS S3: refer to https://docs.rs/object_store/0.11.0/object_store/aws/enum.AmazonS3ConfigKey.html
    ///
    /// Azure Cloud File Storage: refer to https://docs.rs/0.11.0/latest/object_store/azure/enum.AzureConfigKey.html
    ///
    /// Google Cloud Storage: refer to https://docs.rs/object_store/0.11.0/object_store/gcp/enum.GoogleConfigKey.html
    configs: HashMap<String, String>,
}

impl MountDef {
    /// Return a new [`MountDef`] builder.
    pub fn builder() -> MountDefBuilder {
        MountDefBuilder::default()
    }

    /// Validate the mount definition.
    pub fn validate(&self) -> Result<()> {
        MountDefBuilder::from(self).validate()
    }
}

impl MountDefBuilder {
    pub fn uri(&mut self, uri: impl Into<String>) -> &mut Self {
        let mut uri = uri.into();
        #[cfg(not(target_os = "windows"))]
        if !uri.ends_with('/') {
            uri.push('/');
        }
        #[cfg(target_os = "windows")]
        if !uri.ends_with('\\') && !uri.ends_with('/') {
            uri.push('/');
        }
        self.uri = Some(uri);
        self
    }

    fn validate(&self) -> Result<()> {
        if self.mount_path.is_some() {
            let mount_path = self.mount_path.as_ref().unwrap();
            SPath::parse(mount_path).map_err(|e| {
                StorageError::ConfigurationError(format!(
                    "Invalid mount path {} : {}",
                    mount_path, e
                ))
            })?;
        }
        if self.uri.is_some() {
            let uri_str = self.uri.as_ref().unwrap();
            if uri_str.len() != uri_str.trim().len() {
                return Err(StorageError::ConfigurationError(format!(
                    "URI cannot have leading or trailing spaces: '{}'",
                    uri_str
                )));
            }
            #[cfg(not(target_os = "windows"))]
            if !uri_str.ends_with('/') {
                return Err(StorageError::ConfigurationError(format!(
                    "Invalid URI {}, must end with '/'",
                    uri_str
                )));
            }
            #[cfg(target_os = "windows")]
            if !uri_str.ends_with('\\') && !uri_str.ends_with('/') {
                return Err(StorageError::ConfigurationError(format!(
                    "Invalid URI {}, must end with '\\' or '/'",
                    uri_str
                )));
            }
            let uri = Url::parse(uri_str).map_err(|e| {
                StorageError::ConfigurationError(format!("Invalid URI {} : {}", uri_str, e))
            })?;
            match uri.scheme() {
                "file" => {
                    if !is_valid_file_scheme(uri_str) {
                        return Err(StorageError::ConfigurationError(format!(
                            "Invalid file URI, path must be absolute: {}",
                            uri_str
                        )));
                    }
                }
                "s3" => {}
                "az" => {}
                _ => {
                    return Err(StorageError::ConfigurationError(format!(
                        "Unsupported schema {}",
                        uri.scheme()
                    )))
                }
            }
        }
        Ok(())
    }
}

#[cfg(not(target_os = "windows"))]
fn is_valid_file_scheme(uri: &str) -> bool {
    uri.to_lowercase().starts_with("file:///")
}

#[cfg(target_os = "windows")]
fn is_valid_file_scheme(uri: &str) -> bool {
    let pattern = Regex::new(r"^file:///[a-zA-Z]:[/\\]").unwrap();
    pattern.is_match(&uri.to_lowercase())
}

impl From<&MountDef> for MountDefBuilder {
    fn from(mount: &MountDef) -> MountDefBuilder {
        MountDefBuilder {
            mount_path: Some(mount.mount_path.clone()),
            uri: Some(mount.uri.clone()),
            configs: Some(mount.configs.clone()),
        }
    }
}

trait PathMapper {
    fn map(&self, path: &Path) -> Result<Path>;
}

#[derive(Debug)]
struct PathMapperPrefixer {
    prefix_elements: Vec<String>,
}

impl PathMapperPrefixer {
    fn new(prefix_path: &Path) -> Self {
        Self {
            prefix_elements: prefix_path
                .parts()
                .map(|part| part.as_ref().to_string())
                .collect(),
        }
    }
}

impl PathMapper for PathMapperPrefixer {
    fn map(&self, path: &Path) -> Result<Path> {
        let path = match self.prefix_elements.is_empty() {
            true => path.clone(),
            false => {
                let prefix_parts = self
                    .prefix_elements
                    .iter()
                    .map(|p| PathPart::from(p.as_str()));
                let chained = prefix_parts.chain(path.parts());
                Path::from_iter(chained)
            }
        };
        Ok(path)
    }
}

#[derive(Debug)]
struct PathMapperTrimmer {
    elements_to_trim: usize,
}

impl PathMapperTrimmer {
    fn new(elements_to_trim: usize) -> Self {
        Self { elements_to_trim }
    }
}

impl PathMapper for PathMapperTrimmer {
    fn map(&self, path: &Path) -> Result<Path> {
        Ok(Path::from_iter(path.parts().skip(self.elements_to_trim)))
    }
}

type PathMapperFromMount = PathMapperTrimmer;
type PathMapperToMount = PathMapperPrefixer;
type PathMapperToUri = PathMapperPrefixer;
type PathMapperFromUri = PathMapperTrimmer;

/// A mount that is backed by an object store.
pub struct Mount {
    def: MountDef,
    uri_scheme_authority: Url,
    mount_path: SPath,
    path_mapper_from_mount: PathMapperFromMount,
    path_mapper_to_mount: PathMapperToMount,
    path_mapper_to_uri: PathMapperToUri,
    path_mapper_from_uri: PathMapperFromUri,
    store: Box<dyn ObjectStore>,
}

impl Debug for Mount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mount")
            .field("mount_path", &format!("{}/", &self.mount_path))
            .field("store_uri", &self.def.uri())
            .finish()
    }
}

impl Mount {
    fn replace_if_var(val: &str, config_vars: &HashMap<String, String>) -> Result<String> {
        if val.starts_with("$${") && val.ends_with("}") {
            // escaped value, remove one $ and return

            Ok(val[1..].to_string())
        } else if val.starts_with("${") && val.ends_with("}") {
            // extract value from variable and fully replace value

            // we upper case variable names (case insensitive as in WIN env vars)
            let var_name = &val[2..val.len() - 1].to_uppercase();
            if let Some(val) = config_vars.get(var_name) {
                Ok(val.to_string())
            } else {
                return Err(StorageError::ConfigurationError(format!(
                    "Variable {} not found",
                    var_name
                )));
            }
        } else {
            // return value as is

            Ok(val.to_string())
        }
    }

    fn resolve_vars_in_configs(
        configs: &HashMap<String, String>,
        config_vars: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        let mut resolved_configs = HashMap::new();
        for (k, v) in configs.iter() {
            resolved_configs.insert(k.to_string(), Self::replace_if_var(v, config_vars)?);
        }
        Ok(resolved_configs)
    }

    /// Create an object store from the URI and configs.
    fn create_store(
        uri: &Url,
        configs: &HashMap<String, String>,
        config_vars: &HashMap<String, String>,
    ) -> Result<Box<dyn ObjectStore>> {
        let configs = Self::resolve_vars_in_configs(configs, config_vars)?;
        match uri.scheme() {
            "file" | "s3" | "az" => {
                let store = object_store::parse_url_opts(uri, configs)
                    .map_err(StorageError::CouldNotCreateObjectStore)?
                    .0;
                Ok(store)
            }
            _ => Err(StorageError::ConfigurationError(format!(
                "Unsupported schema {}",
                uri.scheme()
            ))),
        }
    }

    /// Create a [`Mount`] with the given definition.
    pub fn new(def: MountDef, vars: &HashMap<String, String>) -> Result<Self> {
        let mut uri = Url::parse(def.uri()).unwrap();
        let store = Self::create_store(&uri, def.configs(), vars)?;

        let mount_path = SPath::parse(def.mount_path())?;
        let path_mapper_from_mount = PathMapperFromMount::new(mount_path.parts().count());
        let path_mapper_to_mount = PathMapperToMount::new(&mount_path);

        let uri_path = Path::parse(uri.abs_path()).unwrap();
        let path_mapper_to_uri = PathMapperToUri::new(&uri_path);
        let path_mapper_from_uri = PathMapperFromUri::new(uri_path.parts().count());

        debug!("Mount, mount: {} uri: {}", def.mount_path(), def.uri());

        uri.set_path("");
        Ok(Mount {
            def,
            uri_scheme_authority: uri,
            mount_path,
            path_mapper_from_mount,
            path_mapper_to_mount,
            path_mapper_to_uri,
            path_mapper_from_uri,
            store,
        })
    }

    pub fn def(&self) -> &MountDef {
        &self.def
    }

    pub fn mount_path(&self) -> &SPath {
        &self.mount_path
    }

    fn to_external_path(&self, path: &Path) -> Result<Path> {
        self.path_mapper_to_uri
            .map(&self.path_mapper_from_mount.map(path)?)
    }

    fn to_mount_path(&self, path: &Path) -> Result<Path> {
        self.path_mapper_to_mount
            .map(&self.path_mapper_from_uri.map(path)?)
    }

    pub fn to_external_uri(&self, path: &SPath) -> Result<Url> {
        let external_path = self.to_external_path(&path.0)?;
        let mut uri = self.uri_scheme_authority.clone();
        uri.set_path(external_path.as_ref());
        Ok(uri)
    }

    pub async fn exists(&self, path: &SPath) -> Result<bool> {
        let external_path = self.to_external_path(&path.0)?;
        match self.store.get_range(&external_path, 0..1).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StorageError::CouldNotReadFromObjectStore(
                external_path.to_string(),
                e,
            )),
        }
    }

    pub async fn delete(&self, path: &SPath) -> Result<()> {
        let external_path = self.to_external_path(&path.0)?;
        match self.store.delete(&external_path).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => {
                Ok(()) // S3 impl does not return NotFound when deleting a non existing file.
                       //                Err(StorageError::NotFound(path.to_string()))
            }
            Err(e) => Err(StorageError::CouldNotDeleteFromObjectStore(
                path.to_string(),
                e,
            )),
        }
    }

    pub async fn write(&self, path: &SPath, data: Vec<u8>) -> Result<()> {
        if path == &self.mount_path {
            return Err(StorageError::InvalidPath(
                path.to_string(),
                format!("Cannot write to {} mount root path", self.mount_path),
            ));
        }
        let external_path = self.to_external_path(&path.0)?;

        match self.store.put(&external_path, PutPayload::from(data)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageError::CouldNotWriteToObjectStore(
                path.to_string(),
                e,
            )),
        }
    }

    pub async fn read(&self, path: &SPath) -> Result<Vec<u8>> {
        let external_path = self.to_external_path(&path.0)?;
        match self.store.get(&external_path).await {
            Ok(data) => match data.bytes().await {
                Ok(bytes) => Ok(bytes.to_vec()),
                Err(e) => Err(StorageError::CouldNotReadFromObjectStore(
                    path.to_string(),
                    e,
                )),
            },
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageError::NotFound(path.to_string()))
            }
            Err(e) => Err(StorageError::CouldNotReadFromObjectStore(
                path.to_string(),
                e,
            )),
        }
    }

    pub async fn read_stream(
        &self,
        path: &SPath,
    ) -> Result<BoxStream<'static, object_store::Result<Bytes>>> {
        let external_path = self.to_external_path(&path.0)?;
        match self.store.get(&external_path).await {
            Ok(res) => Ok(res.into_stream()),
            Err(object_store::Error::NotFound { .. }) => {
                Err(StorageError::NotFound(path.to_string()))
            }
            Err(e) => Err(StorageError::CouldNotReadFromObjectStore(
                path.to_string(),
                e,
            )),
        }
    }

    pub async fn list(&self, path: &SPath) -> Result<Vec<SPath>> {
        let external_path = self.to_external_path(&path.0)?;
        match self.store.list_with_delimiter(Some(&external_path)).await {
            Ok(list) => {
                let files = list
                    .objects
                    .iter()
                    .map(|o| &o.location)
                    .map(|p| SPath(self.to_mount_path(&Path::parse(p).unwrap()).unwrap()))
                    .collect::<Vec<_>>();
                Ok(files)
            }
            Err(e) => Err(StorageError::CouldNotReadFromObjectStore(
                path.to_string(),
                e,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::mount::{Mount, PathMapper, PathMapperPrefixer, PathMapperTrimmer};
    use crate::{MountDef, SPath, StorageError};
    use bytes::Bytes;
    use futures_util::StreamExt;
    use object_store::path::Path;
    use std::collections::HashMap;
    use td_common::absolute_path::AbsolutePath;
    use td_test::reqs::{
        AzureStorageWithAccountKeyReqs, S3WithAccessKeySecretKeyReqs, TestRequirements,
    };
    use testdir::testdir;
    use url::Url;

    fn bar_file() -> String {
        if cfg!(target_os = "windows") {
            "file:///c:/bar".to_string()
        } else {
            "file:///bar".to_string()
        }
    }

    fn slashed_bar_file() -> String {
        if cfg!(target_os = "windows") {
            "file:///c:/bar/".to_string()
        } else {
            "file:///bar/".to_string()
        }
    }

    fn relative_bar_file() -> String {
        "file://bar".to_string()
    }

    fn padded_bar_file() -> String {
        if cfg!(target_os = "windows") {
            " file:///c:/bar".to_string()
        } else {
            " file:///bar".to_string()
        }
    }

    #[test]
    fn test_path_mapper_trimmer() {
        let mount_path = object_store::path::Path::from("mount");
        let trimmer = PathMapperTrimmer::new(mount_path.parts().count());
        let path = object_store::path::Path::from("mount/foo");
        let mapped = trimmer.map(&path).unwrap();
        assert_eq!(mapped, object_store::path::Path::from("foo"));
    }

    #[test]
    fn test_path_mapper_prefixer() {
        let mount_path = object_store::path::Path::from("mount");
        let prefixer = PathMapperPrefixer::new(&mount_path);
        let path = object_store::path::Path::from("foo");
        let mapped = prefixer.map(&path).unwrap();
        assert_eq!(mapped, object_store::path::Path::from("mount/foo"));
    }

    #[test]
    fn test_replace_if_var() {
        let env_vars = HashMap::from([("FOO".to_string(), "bar".to_string())]);
        assert_eq!(
            Mount::replace_if_var("FOO", &env_vars).unwrap(),
            "FOO".to_string()
        );
        assert_eq!(
            Mount::replace_if_var("${FOO}", &env_vars).unwrap(),
            "bar".to_string()
        );
        assert_eq!(
            Mount::replace_if_var("${foo}", &env_vars).unwrap(),
            "bar".to_string()
        );
        assert_eq!(
            Mount::replace_if_var("$${FOO}", &env_vars).unwrap(),
            "${FOO}".to_string()
        );
    }

    #[test]
    fn test_resolve_vars_in_configs() {
        let env_vars = HashMap::from([("STORAGE_MOUNT_A".to_string(), "aa".to_string())]);

        let mut configs = HashMap::new();
        configs.insert("a".to_string(), "${STORAGE_MOUNT_A}".to_string());
        configs.insert("x".to_string(), "x".to_string());

        let merged = Mount::resolve_vars_in_configs(&configs, &env_vars).unwrap();
        assert_eq!(merged["a"], "aa".to_string());
        assert_eq!(merged["x"], "x".to_string());
    }

    #[test]
    fn test_mount_def_validation_ok() {
        let mount_def = MountDef::builder()
            .mount_path("/foo")
            .uri(bar_file())
            .build()
            .unwrap();
        assert_eq!(mount_def.mount_path(), "/foo");
        assert_eq!(mount_def.uri(), &slashed_bar_file());
        assert_eq!(mount_def.configs(), &HashMap::new());
    }

    #[test]
    fn test_def_mount_validation_error() {
        assert!(matches!(
            MountDef::builder()
                .mount_path("foo")
                .uri("foo:///bar")
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .mount_path("/foo")
                .uri(relative_bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .mount_path("/foo/x$")
                .uri(bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .mount_path("/foo/")
                .uri(bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .mount_path("/foo ")
                .uri(bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .mount_path("/foo")
                .uri(padded_bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_mount_def_validate() {
        let json_str = format!(
            r#"{{"mount_path":"/foo","uri":"{}","configs":{{}},"env_prefix":null}}"#,
            slashed_bar_file()
        );
        let mount_def: MountDef = serde_json::from_str(&json_str).unwrap();

        assert!(matches!(mount_def.validate(), Ok(())));

        let json_str = format!(
            r#"{{"mount_path":"foo","uri":"{}","configs":{{}},"env_prefix":null}}"#,
            slashed_bar_file()
        );

        let mount_def: MountDef = serde_json::from_str(&json_str).unwrap();
        assert!(matches!(
            mount_def.validate(),
            Err(StorageError::ConfigurationError(_))
        ));

        let json_str = format!(
            r#"{{"mount_path":"/foo","uri":"{}","configs":{{}},"env_prefix":null}}"#,
            bar_file()
        );
        let mount_def: MountDef = serde_json::from_str(&json_str).unwrap();
        assert!(matches!(
            mount_def.validate(),
            Err(StorageError::ConfigurationError(_))
        ));
    }

    async fn test_mount(
        uri: &Url,
        mount_path: &str,
        store: Box<dyn object_store::ObjectStore>,
        mount: Mount,
    ) {
        assert_eq!(mount.mount_path(), &SPath::parse(mount_path).unwrap());

        // to_external_uri()
        let root_in_mount = SPath::parse(mount_path).unwrap();
        assert_eq!(&mount.to_external_uri(&root_in_mount).unwrap(), uri);
        let child_in_mount = SPath::parse(mount_path).unwrap().child("external").unwrap();

        let uri_external = Url::parse(&format!("{}/{}", uri, "external")).unwrap(); // just because how Url::join() works
        assert_eq!(
            mount.to_external_uri(&child_in_mount).unwrap(),
            uri_external
        );

        // exits()
        let file_in_mount = SPath::parse(mount_path).unwrap().child("exists").unwrap();
        let res = mount.exists(&file_in_mount).await;
        assert!(matches!(res, Ok(false)));
        let path_in_store = Path::parse(uri.abs_path()).unwrap().child("exists");
        store
            .put(&path_in_store, object_store::PutPayload::from(vec![1]))
            .await
            .unwrap();
        assert!(matches!(mount.exists(&file_in_mount).await, Ok(true)));

        // delete()
        let file_in_mount = SPath::parse(mount_path).unwrap().child("delete").unwrap();
        let path_in_store = Path::parse(uri.abs_path()).unwrap().child("delete");
        store
            .put(&path_in_store, object_store::PutPayload::from(vec![1]))
            .await
            .unwrap();
        assert!(matches!(mount.delete(&file_in_mount).await, Ok(())));

        // S3 does not return NotFound when deleting a non existing file
        // assert!(matches!(
        //     mount.delete(&file_in_mount).await,
        //     Err(StorageError::NotFound(_))
        // ));

        // write()
        let mount_as_file = SPath::parse(mount_path).unwrap();
        assert!(matches!(
            mount.write(&mount_as_file, vec![2]).await,
            Err(StorageError::InvalidPath(_, _))
        ));

        let file_in_mount = SPath::parse(mount_path).unwrap().child("write").unwrap();
        let path_in_store = Path::parse(uri.abs_path()).unwrap().child("write");
        assert!(matches!(mount.write(&file_in_mount, vec![1]).await, Ok(())));
        let get = store
            .get(&path_in_store)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        assert_eq!(get, vec![1]);

        // read()
        let file_in_mount = SPath::parse(mount_path).unwrap().child("read").unwrap();
        let path_in_store = Path::parse(uri.abs_path()).unwrap().child("read");
        store
            .put(&path_in_store, object_store::PutPayload::from(vec![1]))
            .await
            .unwrap();
        let data = mount.read(&file_in_mount).await.unwrap();
        assert_eq!(data, vec![1]);
        let file_in_mount = SPath::parse(mount_path)
            .unwrap()
            .child("read_not_there")
            .unwrap();
        assert!(matches!(
            mount.read(&file_in_mount).await,
            Err(StorageError::NotFound(_))
        ));

        // read_stream()
        let file_in_mount = SPath::parse(mount_path)
            .unwrap()
            .child("read-stream")
            .unwrap();
        let path_in_store = Path::parse(uri.abs_path()).unwrap().child("read-stream");
        store
            .put(&path_in_store, object_store::PutPayload::from(vec![1]))
            .await
            .unwrap();
        let stream = mount.read_stream(&file_in_mount).await.unwrap();
        let got = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(got, vec![Bytes::from(vec![1])]);

        // list()
        let dir_in_mount = SPath::parse(mount_path).unwrap().child("list").unwrap();
        let file_in_mount = dir_in_mount.child("file").unwrap();
        let path_in_store = Path::parse(uri.abs_path())
            .unwrap()
            .child("list")
            .child("file");
        store
            .put(&path_in_store, object_store::PutPayload::from(vec![1]))
            .await
            .unwrap();
        let files = mount.list(&dir_in_mount).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], file_in_mount);
    }

    #[tokio::test]
    async fn test_file_root_mount() {
        let test_dir = testdir!();

        #[cfg(target_os = "windows")]
        let uri = format!("file:///{}", test_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let uri = format!("file://{}", test_dir.to_string_lossy());

        let mount_def = MountDef::builder()
            .mount_path("/")
            .uri(&uri)
            .build()
            .unwrap();
        let uri = Url::parse(&uri).unwrap();
        let store = object_store::parse_url(&uri).unwrap().0;
        test_mount(
            &uri,
            "/",
            store,
            Mount::new(mount_def, &HashMap::new()).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_file_non_root_mount() {
        let test_dir = testdir!();

        #[cfg(target_os = "windows")]
        let uri = format!("file:///{}", test_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let uri = format!("file://{}", test_dir.to_string_lossy());

        let mount_def = MountDef::builder()
            .mount_path("/mount")
            .uri(&uri)
            .build()
            .unwrap();
        let uri = Url::parse(&uri).unwrap();
        let store = object_store::parse_url(&uri).unwrap().0;
        test_mount(
            &uri,
            "/mount",
            store,
            Mount::new(mount_def, &HashMap::new()).unwrap(),
        )
        .await;
    }

    async fn test_aws_mount(mount_path: &str, s3_info: &S3WithAccessKeySecretKeyReqs) {
        let configs = HashMap::from([
            ("aws_region".to_string(), s3_info.region.clone()),
            ("aws_access_key_id".to_string(), s3_info.access_key.clone()),
            (
                "aws_secret_access_key".to_string(),
                s3_info.secret_key.clone(),
            ),
        ]);

        let uri = format!("{}/{}", s3_info.uri, s3_info.test_path().to_str().unwrap());
        let uri = Url::parse(&uri).unwrap();

        let object_store = object_store::parse_url_opts(&uri, &configs).unwrap().0;

        let mount_def = MountDef::builder()
            .mount_path(mount_path)
            .uri(uri.to_string())
            .configs(configs.clone())
            .build()
            .unwrap();

        test_mount(
            &uri,
            mount_path,
            object_store,
            Mount::new(mount_def, &configs).unwrap(),
        )
        .await;
    }

    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s30"))]
    async fn test_s3_root_mount(reqas: S3WithAccessKeySecretKeyReqs) {
        test_aws_mount("/", &reqas).await;
    }

    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s30"))]
    async fn test_s3_non_root_mount(reqs: S3WithAccessKeySecretKeyReqs) {
        test_aws_mount("/foo", &reqs).await;
    }

    async fn test_azure_mount(mount_path: &str, az_info: &AzureStorageWithAccountKeyReqs) {
        let configs = HashMap::from([
            (
                "azure_storage_account_name".to_string(),
                az_info.account_name.clone(),
            ),
            (
                "azure_storage_account_key".to_string(),
                az_info.account_key.clone(),
            ),
        ]);

        let uri = format!("{}/{}", az_info.uri, az_info.test_path().to_str().unwrap());
        let uri = Url::parse(&uri).unwrap();

        let object_store = object_store::parse_url_opts(&uri, &configs).unwrap().0;

        let mount_def = MountDef::builder()
            .mount_path(mount_path)
            .uri(uri.to_string())
            .configs(configs.clone())
            .build()
            .unwrap();

        test_mount(
            &uri,
            mount_path,
            object_store,
            Mount::new(mount_def, &configs).unwrap(),
        )
        .await;
    }

    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az0"))]
    async fn test_azure_root_mount(reqs: AzureStorageWithAccountKeyReqs) {
        test_azure_mount("/", &reqs).await;
    }

    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az0"))]
    async fn test_azure_non_root_mount(reqs: AzureStorageWithAccountKeyReqs) {
        test_azure_mount("/foo", &reqs).await;
    }
}
