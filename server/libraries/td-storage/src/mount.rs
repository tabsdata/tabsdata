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

    #[builder(default)]
    /// Environment variables prefixed with [`env_prefix`] are added (with precedence) to the configs.
    ///
    /// The [`env_prefix`] prefix is removed and the rest  of the environment variable name is lowercased.
    env_prefix: Option<String>,
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
            env_prefix: Some(mount.env_prefix.clone()),
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
    /// Merge environment variables with the configs.
    fn merge_envs_into_configs(
        env_prefix: &Option<String>,
        configs: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut configs = configs;
        if let Some(env_prefix) = env_prefix {
            for (key, value) in std::env::vars() {
                if let Some(key) = key.strip_prefix(env_prefix) {
                    configs.insert(key.to_lowercase(), value);
                }
            }
        }
        configs
    }

    /// Create an object store from the URI and configs.
    fn create_store(
        uri: &Url,
        configs: &HashMap<String, String>,
        env_prefix: &Option<String>,
    ) -> Result<Box<dyn ObjectStore>> {
        let configs = Self::merge_envs_into_configs(env_prefix, configs.clone());
        match uri.scheme() {
            "file" | "s3" => {
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
    pub fn new(def: MountDef) -> Result<Self> {
        let mut uri = Url::parse(def.uri()).unwrap();
        let store = Self::create_store(&uri, def.configs(), def.env_prefix())?;

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
    fn test_merge_envs_into_configs() {
        std::env::set_var("STORAGE_MOUNT_A", "aa");

        let mut configs = HashMap::new();
        configs.insert("a".to_string(), "a".to_string());
        configs.insert("x".to_string(), "x".to_string());

        let merged =
            Mount::merge_envs_into_configs(&Some("STORAGE_MOUNT_".to_string()), configs.clone());
        assert_eq!(merged["a"], "aa".to_string());
        assert_eq!(merged["x"], "x".to_string());
        std::env::remove_var("STORAGE_MOUNT_A");
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
        assert_eq!(mount_def.env_prefix(), &None);

        let mount_def = MountDef::builder()
            .mount_path("/foo")
            .uri(slashed_bar_file())
            .configs(HashMap::from([(String::from("foo"), String::from("bar"))]))
            .env_prefix("FOO")
            .build()
            .unwrap();
        assert_eq!(mount_def.mount_path(), "/foo");
        assert_eq!(mount_def.uri(), &slashed_bar_file());
        assert_eq!(
            mount_def.configs(),
            &HashMap::from([(String::from("foo"), String::from("bar"))])
        );
        assert_eq!(mount_def.env_prefix(), &Some("FOO".to_string()));
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
        assert!(matches!(mount.exists(&file_in_mount).await, Ok(false)));
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
        test_mount(&uri, "/", store, Mount::new(mount_def).unwrap()).await;
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
        test_mount(&uri, "/mount", store, Mount::new(mount_def).unwrap()).await;
    }

    async fn test_if_envs(mount: &str, env_prefix: &str, envs: &[&str]) {
        let r = envs
            .iter()
            .map(std::env::var)
            .collect::<Result<Vec<_>, _>>();
        if r.is_ok() {
            let uri = std::env::var(envs[0]).unwrap();

            let mount_def = MountDef::builder()
                .mount_path(mount)
                .uri(&uri)
                .env_prefix(env_prefix)
                .build()
                .unwrap();

            let uri = Url::parse(&uri).unwrap();
            let configs =
                Mount::merge_envs_into_configs(&Some(env_prefix.to_string()), HashMap::new());

            let store = object_store::parse_url_opts(&uri, configs).unwrap().0;

            test_mount(&uri, mount, store, Mount::new(mount_def).unwrap()).await;
        }
    }

    #[tokio::test]
    async fn test_s3_root_mount() {
        let vars = vec![
            "TEST_MOUNT_S3",
            "TEST_MOUNT_AWS_REGION",
            "TEST_MOUNT_AWS_ACCESS_KEY_ID",
            "TEST_MOUNT_AWS_SECRET_ACCESS_KEY",
        ];
        test_if_envs("/", "TEST_MOUNT_", &vars).await;
    }

    #[tokio::test]
    async fn test_s3_non_root_mount() {
        let vars = vec![
            "TEST_MOUNT_S3",
            "TEST_MOUNT_AWS_REGION",
            "TEST_MOUNT_AWS_ACCESS_KEY_ID",
            "TEST_MOUNT_AWS_SECRET_ACCESS_KEY",
        ];
        test_if_envs("/foo", "TEST_MOUNT_", &vars).await;
    }
}
