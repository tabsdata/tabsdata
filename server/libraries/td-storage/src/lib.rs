//
// Copyright 2024 Tabs Data Inc.
//

use bytes::Bytes;
use derive_builder::UninitializedFieldError;
use futures_util::stream::BoxStream;
use itertools::Itertools;
use lazy_static::lazy_static;
use object_store::path::Path;
use regex::Regex;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::path::MAIN_SEPARATOR;
use td_error::td_error;
use tracing::{trace, warn};
use url::Url;

pub mod location;
mod mount;
mod store;

pub use mount::MountDef;
pub use store::MountsStorage;

/// Errors that can occur when interacting with storage.
#[td_error]
pub enum StorageError {
    #[error("Configuration error: {0}")]
    ConfigurationError(String) = 0,
    #[error("Could not create ObjectStore: {0}")]
    CouldNotCreateObjectStore(#[source] object_store::Error) = 1,
    #[error("Invalid path {0}: {1}")]
    InvalidPath(String, String) = 2,
    #[error("Path {0} not in mount {0}")]
    PathNotInMount(String, String) = 3,
    #[error("Path element name {0} must be an ASCII alphanumeric plus . - _ and not more than 100 characters long")]
    InvalidPathElement(String) = 4,
    #[error("Could not write {0}: {1}")]
    CouldNotWriteToObjectStore(String, #[source] object_store::Error) = 5,
    #[error("Could not read {0}: {1}")]
    CouldNotReadFromObjectStore(String, #[source] object_store::Error) = 6,
    #[error("Could not delete {0}: {1}")]
    CouldNotDeleteFromObjectStore(String, #[source] object_store::Error) = 7,
    #[error("Already exists {0}")]
    AlreadyExists(String) = 8,
    #[error("Not found {0}")]
    NotFound(String) = 9,
}

impl From<UninitializedFieldError> for StorageError {
    fn from(ufe: UninitializedFieldError) -> StorageError {
        StorageError::ConfigurationError(ufe.to_string())
    }
}

/// A path in storage.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct SPath(Path);

impl SPath {
    const MAX_ELEMENT_LENGTH: usize = 100;

    fn assert_path_element_name(name: &str) -> Result<()> {
        lazy_static! {
            static ref ELEMENT_NAME_REGEX: Regex = Regex::new(&format!(
                "^([0-9A-Za-z_.-]){{1,{}}}$",
                SPath::MAX_ELEMENT_LENGTH
            ))
            .unwrap();
        }
        if !ELEMENT_NAME_REGEX.is_match(name) {
            return Err(StorageError::InvalidPathElement(name.to_string()));
        }
        Ok(())
    }

    /// Parse a path from a string.
    ///
    /// The path must be absolute and not end with a slash. Only exception is the root path `/`.
    ///
    /// Each path element must be ASCII alphanumeric plus . - _ and not more than 100 characters long.
    pub fn parse(path: impl AsRef<str>) -> Result<SPath> {
        let path = path.as_ref();
        if path.is_empty() {
            return Err(StorageError::InvalidPath(
                path.to_string(),
                "path cannot be empty".to_string(),
            ));
        }
        if !path.starts_with(MAIN_SEPARATOR) {
            return Err(StorageError::InvalidPath(
                path.to_string(),
                "path must be absolute".to_string(),
            ));
        }
        if path.len() > 1 && path.ends_with(MAIN_SEPARATOR) {
            return Err(StorageError::InvalidPath(
                path.to_string(),
                "path cannot end with /".to_string(),
            ));
        }
        let fs_path = Path::parse(path)
            .map_err(|e| StorageError::InvalidPath(path.to_string(), e.to_string()))?;
        for part in fs_path.parts() {
            Self::assert_path_element_name(part.as_ref())
                .map_err(|e| StorageError::InvalidPath(path.to_string(), e.to_string()))?;
        }
        Ok(SPath(fs_path))
    }

    /// Return the last element of the path.
    ///
    /// Returns `None` if the path is root.
    pub fn last_element(&self) -> Option<&str> {
        self.0.filename()
    }

    /// Return the extension of the last element of the path.
    ///
    /// Returns `None` if the path is root or the last element has no extension.
    pub fn extension(&self) -> Option<&str> {
        self.0.extension()
    }

    /// Return the parent of the path.
    ///
    /// Returns `None` if the path is root.
    pub fn parent(&self) -> Option<SPath> {
        let mut parts = self.0.parts().collect_vec();
        if parts.pop().is_some() {
            Some(SPath(Path::from_iter(parts)))
        } else {
            None
        }
    }

    /// Create a child path with given name as last path element.
    pub fn child(&self, name: &str) -> Result<SPath> {
        Self::assert_path_element_name(name)?;
        Ok(SPath(self.0.child(name)))
    }
}

impl Deref for SPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for SPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "/{}", self.0)
    }
}

/// Result type for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub struct Storage {
    storage: MountsStorage,
}

impl Storage {
    pub async fn from(mount_defs: Vec<MountDef>) -> Result<Self> {
        let storage = MountsStorage::from(mount_defs).await?;
        Ok(Self { storage })
    }

    pub fn to_external_uri(&self, path: &SPath) -> Result<Url> {
        let res = self.storage.to_external_uri(path);
        match &res {
            Ok(uri) => trace!("to_external_uri({}) -> {}", path, uri),
            Err(e) => warn!("to_external_uri({}) error: {}", path, e),
        }
        res
    }

    pub async fn exists(&self, path: &SPath) -> Result<bool> {
        let res = self.storage.exists(path).await;
        match &res {
            Ok(exists) => trace!("exists({}) -> {}", path, exists),
            Err(e) => warn!("exists({}) error: {}", path, e),
        }
        res
    }

    pub async fn delete(&self, path: &SPath) -> Result<()> {
        let res = self.storage.delete(path).await;
        match &res {
            Ok(_) => trace!("delete({}) -> ok", path),
            Err(e) => warn!("delete({}) error: {}", path, e),
        }
        res
    }

    pub async fn write(&self, path: &SPath, data: Vec<u8>) -> Result<()> {
        let res = self.storage.write(path, data).await;
        match &res {
            Ok(_) => trace!("write({}) -> ok", path),
            Err(e) => warn!("write({}) error: {}", path, e),
        }
        res
    }

    pub async fn read(&self, path: &SPath) -> Result<Vec<u8>> {
        let res = self.storage.read(path).await;
        match &res {
            Ok(_) => trace!("read({}) -> ok", path),
            Err(e) => warn!("read({}) error: {}", path, e),
        }
        res
    }

    pub async fn read_stream(
        &self,
        path: &SPath,
    ) -> Result<BoxStream<'static, object_store::Result<Bytes>>> {
        let res = self.storage.read_stream(path).await;
        match &res {
            Ok(_) => trace!("read_stream({}) -> ok", path),
            Err(e) => warn!("read_stream({}) error: {}", path, e),
        }
        res
    }

    pub async fn list(&self, path: &SPath) -> Result<Vec<SPath>> {
        let res = self.storage.list(path).await;
        match &res {
            Ok(_) => trace!("list({}) -> ok", path),
            Err(e) => warn!("list({}) error: {}", path, e),
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use crate::{MountDef, SPath, Storage};
    use object_store::path::Path;
    use std::fs;
    use std::ops::Deref;
    use testdir::testdir;

    #[test]
    fn test_spath_assert_path_element_name() {
        assert!(SPath::assert_path_element_name("1").is_ok());
        assert!(SPath::assert_path_element_name("a-").is_ok());
        assert!(SPath::assert_path_element_name("a_").is_ok());
        assert!(SPath::assert_path_element_name("a.ext").is_ok());
        assert!(
            SPath::assert_path_element_name(&String::from_utf8(vec![b'a'; 100]).unwrap()).is_ok()
        );

        assert!(SPath::assert_path_element_name("").is_err());
        assert!(SPath::assert_path_element_name(" a").is_err());
        assert!(SPath::assert_path_element_name("a ").is_err());
        assert!(SPath::assert_path_element_name("a b").is_err());
        assert!(SPath::assert_path_element_name("!").is_err());
        assert!(
            SPath::assert_path_element_name(&String::from_utf8(vec![b'a'; 101]).unwrap()).is_err()
        );
    }

    #[test]
    fn test_spath_parse() {
        assert!(SPath::parse("/").is_ok());
        assert!(SPath::parse("/a").is_ok());
        assert!(SPath::parse("/a/b").is_ok());
        assert!(SPath::parse("/a/b.ext").is_ok());

        assert!(SPath::parse("").is_err());
        assert!(SPath::parse("a").is_err());
        assert!(SPath::parse("a/").is_err());
        assert!(SPath::parse("a/b").is_err());
        assert!(SPath::parse("a/b/").is_err());
        assert!(SPath::parse("a//b").is_err());
        assert!(SPath::parse("/a#b").is_err()); // invalid path element name
    }

    #[test]
    fn test_spath_deref_to_path() {
        assert_eq!(SPath::parse("/").unwrap().deref(), &Path::default());
    }

    #[test]
    fn test_spath_display() {
        assert_eq!(SPath::parse("/foo").unwrap().to_string(), "/foo");
    }

    #[tokio::test]
    async fn test_storage_api() {
        let test_dir = testdir!();
        let mount1_dir = test_dir.join("mount1");
        fs::create_dir(&mount1_dir).unwrap();
        let mount2_dir = test_dir.join("mount2");
        fs::create_dir(&mount2_dir).unwrap();

        #[cfg(target_os = "windows")]
        let uri1 = format!("file:///{}", mount1_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let uri1 = format!("file://{}", mount1_dir.to_string_lossy());

        let mount1 = MountDef::builder()
            .mount_path("/")
            .uri(uri1)
            .build()
            .unwrap();

        #[cfg(target_os = "windows")]
        let uri2 = format!("file:///{}", mount2_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let uri2 = format!("file://{}", mount2_dir.to_string_lossy());

        let mount2 = MountDef::builder()
            .mount_path("/foo")
            .uri(uri2)
            .build()
            .unwrap();
        let storage = Storage::from(vec![mount1, mount2]).await.unwrap();

        #[cfg(target_os = "windows")]
        let match1 = format!("file:///{}", mount1_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let match1 = format!("file://{}", mount1_dir.to_string_lossy());

        assert_eq!(
            storage
                .to_external_uri(&SPath::parse("/").unwrap())
                .unwrap()
                .as_str(),
            &match1.replace("\\", "/")
        );

        #[cfg(target_os = "windows")]
        let match2 = format!("file:///{}/foo.txt", mount1_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let match2 = format!("file://{}/foo.txt", mount1_dir.to_string_lossy());

        assert_eq!(
            storage
                .to_external_uri(&SPath::parse("/foo.txt").unwrap())
                .unwrap()
                .as_str(),
            &match2.replace("\\", "/")
        );

        #[cfg(target_os = "windows")]
        let match3 = format!("file:///{}/bar.txt", mount2_dir.to_string_lossy());
        #[cfg(not(target_os = "windows"))]
        let match3 = format!("file://{}/bar.txt", mount2_dir.to_string_lossy());

        assert_eq!(
            storage
                .to_external_uri(&SPath::parse("/foo/bar.txt").unwrap())
                .unwrap()
                .as_str(),
            &match3.replace("\\", "/")
        );
    }
}
