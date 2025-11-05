//
// Copyright 2024 Tabs Data Inc.
//

use super::{Result, SPath, StorageError};
use bytes::Bytes;
use derive_builder::Builder;
use futures_util::TryStreamExt;
use futures_util::stream::BoxStream;
use object_store::path::{Path, PathPart};
use object_store::{ObjectStore, PutPayload};
#[cfg(target_os = "windows")]
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;
use td_common::absolute_path::AbsolutePath;
use tracing::debug;
use url::Url;

/// Definition of a mount.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(
    setter(into, strip_option),
    build_fn(validate = "Self::validate", error = "StorageError")
)]
pub struct MountDef {
    /// A unique identifier for the mount, it must be an ascii word, it should never change for a mount.
    pub id: String,

    /// Path, in the storage, where the mount is located.
    pub path: String,

    #[builder(setter(custom))]
    /// External URI that is backing the storage of the mount.
    pub uri: String,

    #[builder(default)]
    /// Options for the mount. This is [`uri`] scheme specific.
    ///
    /// AWS S3: refer to https://docs.rs/object_store/0.11.0/object_store/aws/enum.AmazonS3ConfigKey.html
    ///
    /// Azure Cloud File Storage: refer to https://docs.rs/0.11.0/latest/object_store/azure/enum.AzureConfigKey.html
    ///
    /// Google Cloud Storage: refer to https://docs.rs/object_store/0.11.0/object_store/gcp/enum.GoogleConfigKey.html
    options: Option<HashMap<String, String>>,
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

    /// Mount ID as prefix (uppercased and appended with `_`). It can be used to
    /// create environment variables with information for the mount for sub-processes.
    pub fn id_as_prefix(&self) -> String {
        format!("{}_", self.id.to_uppercase())
    }

    pub fn options(&self) -> &HashMap<String, String> {
        static NO_OPTIONS: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        self.options.as_ref().unwrap_or(&NO_OPTIONS)
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
        if self.path.is_some() {
            let mount_path = self.path.as_ref().unwrap();
            SPath::parse(mount_path).map_err(|e| {
                StorageError::ConfigurationError(format!("Invalid mount path {mount_path} : {e}"))
            })?;
        }
        if self.uri.is_some() {
            let uri_str = self.uri.as_ref().unwrap();
            if uri_str.len() != uri_str.trim().len() {
                return Err(StorageError::ConfigurationError(format!(
                    "URI cannot have leading or trailing spaces: '{uri_str}'"
                )));
            }
            #[cfg(not(target_os = "windows"))]
            if !uri_str.ends_with('/') {
                return Err(StorageError::ConfigurationError(format!(
                    "Invalid URI {uri_str}, must end with '/'"
                )));
            }
            #[cfg(target_os = "windows")]
            if !uri_str.ends_with('\\') && !uri_str.ends_with('/') {
                return Err(StorageError::ConfigurationError(format!(
                    "Invalid URI {uri_str}, must end with '\\' or '/'"
                )));
            }
            let uri = Url::parse(uri_str).map_err(|e| {
                StorageError::ConfigurationError(format!("Invalid URI {uri_str} : {e}"))
            })?;
            match uri.scheme() {
                "file" => {
                    if !is_valid_file_scheme(uri_str) {
                        return Err(StorageError::ConfigurationError(format!(
                            "Invalid file URI, path must be absolute: {uri_str}"
                        )));
                    }
                }
                "s3" => {}
                "az" => {}
                "gs" => {}
                _ => {
                    return Err(StorageError::ConfigurationError(format!(
                        "Unsupported schema {}",
                        uri.scheme()
                    )));
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
            id: Some(mount.id.clone()),
            path: Some(mount.path.clone()),
            uri: Some(mount.uri.clone()),
            options: Some(mount.options.clone()),
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
            .field("store_uri", &self.def.uri)
            .finish()
    }
}

impl Mount {
    /// Create an object store from the URI and configs.
    fn create_store(uri: &Url, configs: &HashMap<String, String>) -> Result<Box<dyn ObjectStore>> {
        match uri.scheme() {
            "file" | "s3" | "az" | "gs" => {
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
        let mut uri = Url::parse(&def.uri).unwrap();
        let store = Self::create_store(&uri, def.options())?;

        let mount_path = SPath::parse(&def.path)?;
        let path_mapper_from_mount = PathMapperFromMount::new(mount_path.parts().count());
        let path_mapper_to_mount = PathMapperToMount::new(&mount_path);

        let uri_path = Path::parse(uri.abs_path()).unwrap();
        let path_mapper_to_uri = PathMapperToUri::new(&uri_path);
        let path_mapper_from_uri = PathMapperFromUri::new(uri_path.parts().count());

        debug!("Mount, mount: {} uri: {}", &def.path, &def.uri);

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
                Ok(())
                // S3 impl does not return NotFound when deleting a non existing file.
                // Err(StorageError::NotFound(path.to_string()))
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

    pub async fn read_stream(&self, path: &SPath) -> Result<BoxStream<'static, Result<Bytes>>> {
        let external_path = self.to_external_path(&path.0)?;
        match self.store.get(&external_path).await {
            Ok(res) => {
                let stream = res.into_stream().map_err(StorageError::StreamError);
                Ok(Box::pin(stream))
            }
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
        AzureStorageWithAccountKeyReqs, GcpStorageWithServiceAccountKeyReqs,
        S3WithAccessKeySecretKeyReqs, TestRequirements,
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
    fn test_mount_def_validation_ok() {
        let mount_def = MountDef::builder()
            .id("id")
            .path("/foo")
            .uri(bar_file())
            .build()
            .unwrap();
        assert_eq!(mount_def.path, "/foo");
        assert_eq!(mount_def.uri, slashed_bar_file());
        assert_eq!(mount_def.options(), &HashMap::new());
        assert_eq!(mount_def.id_as_prefix(), "ID_".to_uppercase());
    }

    #[test]
    fn test_def_mount_validation_error() {
        assert!(matches!(
            MountDef::builder().path("foo").uri("foo:///bar").build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .path("/foo")
                .uri(relative_bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder().path("/foo/x$").uri(bar_file()).build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder().path("/foo/").uri(bar_file()).build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder().path("/foo ").uri(bar_file()).build(),
            Err(StorageError::ConfigurationError(_))
        ));
        assert!(matches!(
            MountDef::builder()
                .path("/foo")
                .uri(padded_bar_file())
                .build(),
            Err(StorageError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_mount_def_validate() {
        let json_str = format!(
            r#"{{"id":"id","path":"/foo","uri":"{}","options":{{}},"env_prefix":null}}"#,
            slashed_bar_file()
        );
        let mount_def: MountDef = serde_json::from_str(&json_str).unwrap();

        assert!(matches!(mount_def.validate(), Ok(())));

        let json_str = format!(
            r#"{{"id":"id","path":"foo","uri":"{}","options":{{}},"env_prefix":null}}"#,
            slashed_bar_file()
        );

        let mount_def: MountDef = serde_json::from_str(&json_str).unwrap();
        assert!(matches!(
            mount_def.validate(),
            Err(StorageError::ConfigurationError(_))
        ));

        let json_str = format!(
            r#"{{"id":"id","path":"/foo","uri":"{}","options":{{}},"env_prefix":null}}"#,
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
        assert_eq!(
            mount
                .to_external_uri(&root_in_mount)
                .unwrap()
                .to_string()
                .replace("\\", "/"),
            uri.to_string().replace("\\", "/")
        );
        let child_in_mount = SPath::parse(mount_path).unwrap().child("external").unwrap();

        let uri_external = Url::parse(&format!("{}/{}", uri, "external")).unwrap(); // just because how Url::join() works
        assert_eq!(
            mount
                .to_external_uri(&child_in_mount)
                .unwrap()
                .to_string()
                .replace("\\", "/"),
            uri_external.to_string().replace("\\", "/")
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

        // S3 does not return NotFound when deleting a non-existing file
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
            .id("id")
            .path("/")
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
            .id("id")
            .path("/mount")
            .uri(&uri)
            .build()
            .unwrap();
        let uri = Url::parse(&uri).unwrap();
        let store = object_store::parse_url(&uri).unwrap().0;
        test_mount(&uri, "/mount", store, Mount::new(mount_def).unwrap()).await;
    }

    async fn test_aws_mount(path: &str, s3_info: &S3WithAccessKeySecretKeyReqs) {
        let options = HashMap::from([
            ("aws_region".to_string(), s3_info.region.clone()),
            ("aws_access_key_id".to_string(), s3_info.access_key.clone()),
            (
                "aws_secret_access_key".to_string(),
                s3_info.secret_key.clone(),
            ),
        ]);

        let uri = format!("{}/{}", s3_info.uri, s3_info.test_path().to_str().unwrap());
        let uri = Url::parse(&uri).unwrap();

        let object_store = object_store::parse_url_opts(&uri, &options).unwrap().0;

        let mount_def = MountDef::builder()
            .id("id")
            .path(path)
            .uri(uri.to_string())
            .options(options)
            .build()
            .unwrap();

        test_mount(&uri, path, object_store, Mount::new(mount_def).unwrap()).await;
    }

    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s30"))]
    #[tokio::test]
    async fn test_s3_root_mount(reqas: S3WithAccessKeySecretKeyReqs) {
        test_aws_mount("/", &reqas).await;
    }

    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s30"))]
    #[tokio::test]
    async fn test_s3_non_root_mount(reqs: S3WithAccessKeySecretKeyReqs) {
        test_aws_mount("/foo", &reqs).await;
    }

    async fn test_azure_mount(path: &str, az_info: &AzureStorageWithAccountKeyReqs) {
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
            .id("id")
            .path(path)
            .uri(uri.to_string())
            .options(configs)
            .build()
            .unwrap();

        test_mount(&uri, path, object_store, Mount::new(mount_def).unwrap()).await;
    }

    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az0"))]
    #[tokio::test]
    async fn test_azure_root_mount(reqs: AzureStorageWithAccountKeyReqs) {
        test_azure_mount("/", &reqs).await;
    }

    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az0"))]
    #[tokio::test]
    async fn test_azure_non_root_mount(reqs: AzureStorageWithAccountKeyReqs) {
        test_azure_mount("/foo", &reqs).await;
    }

    async fn test_gcp_mount(path: &str, gcp_info: &GcpStorageWithServiceAccountKeyReqs) {
        let configs = HashMap::from([(
            "google_service_account_key".to_string(),
            gcp_info.service_account_key.to_string(),
        )]);

        let uri = format!(
            "{}/{}",
            gcp_info.uri,
            gcp_info.test_path().to_str().unwrap()
        );
        let uri = Url::parse(&uri).unwrap();

        let object_store = object_store::parse_url_opts(&uri, &configs);
        let object_store = object_store.unwrap().0;

        let mount_def = MountDef::builder()
            .id("id")
            .path(path)
            .uri(uri.to_string())
            .options(configs)
            .build()
            .unwrap();

        test_mount(&uri, path, object_store, Mount::new(mount_def).unwrap()).await;
    }

    #[td_test::test(when(reqs = GcpStorageWithServiceAccountKeyReqs, env_prefix= "gcp0"))]
    #[tokio::test]
    async fn test_gcp_root_mount(reqs: GcpStorageWithServiceAccountKeyReqs) {
        test_gcp_mount("/", &reqs).await;
    }

    #[td_test::test(when(reqs = GcpStorageWithServiceAccountKeyReqs, env_prefix= "gcp0"))]
    #[tokio::test]
    async fn test_gcp_non_root_mount(reqs: GcpStorageWithServiceAccountKeyReqs) {
        test_gcp_mount("/foo", &reqs).await;
    }
}
