//
// Copyright 2024 Tabs Data Inc.
//

use super::{Result, SPath, StorageError};
use crate::mount::{Mount, MountDef};
use bytes::Bytes;
use futures_util::stream::BoxStream;
use object_store::path::{Path, PathPart};
use std::collections::HashMap;
use url::Url;

/// Persistent store based on Storage mounts.
///
/// Mounts behavior is identical to the Unix file system, where the root mount is `/`.
#[derive(Debug)]
pub struct MountsStorage {
    mounts: HashMap<SPath, Mount>,
}

impl MountsStorage {
    /// Create a new Store from a list of MountDefs. There must be definition for the root mount `/`.
    pub async fn from(mount_defs: Vec<MountDef>, vars: &HashMap<String, String>) -> Result<Self> {
        let mut has_root = false;
        static ROOT: &str = "/";
        for mount_def in mount_defs.iter() {
            mount_def.validate()?;
            if mount_def.mount_path() == ROOT {
                has_root = true;
            }
        }
        if !has_root {
            return Err(StorageError::ConfigurationError(
                "No root mount found".to_string(),
            ));
        }
        let mut fs_mounts = HashMap::new();
        for mount_def in mount_defs {
            let mount = Mount::new(mount_def, vars)?;
            fs_mounts.insert(mount.mount_path().clone(), mount);
        }
        Ok(Self { mounts: fs_mounts })
    }

    /// Get the mount definitions.
    pub fn mount_defs(&self) -> Vec<&MountDef> {
        self.mounts.values().map(|mount| mount.def()).collect()
    }

    /// Find the mount for the given path.
    fn find_mount(&self, path: &SPath) -> &Mount {
        let mut current_path = path.clone();
        loop {
            if let Some(mount) = self.mounts.get(&current_path) {
                return mount;
            }
            let mut parts: Vec<PathPart> = current_path.parts().collect();
            parts.pop();
            current_path = SPath(Path::from_iter(parts));
        }
    }

    pub fn to_external_uri(&self, path: &SPath) -> Result<Url> {
        let mount = self.find_mount(path);
        mount.to_external_uri(path)
    }

    pub async fn exists(&self, path: &SPath) -> Result<bool> {
        let mount = self.find_mount(path);
        mount.exists(path).await
    }

    pub async fn delete(&self, path: &SPath) -> Result<()> {
        let mount = self.find_mount(path);
        mount.delete(path).await
    }

    pub async fn write(&self, path: &SPath, data: Vec<u8>) -> Result<()> {
        let mount = self.find_mount(path);
        mount.write(path, data).await
    }

    pub async fn read(&self, path: &SPath) -> Result<Vec<u8>> {
        let mount = self.find_mount(path);
        mount.read(path).await
    }

    pub async fn read_stream(
        &self,
        path: &SPath,
    ) -> Result<BoxStream<'static, object_store::Result<Bytes>>> {
        let mount = self.find_mount(path);
        mount.read_stream(path).await
    }

    pub async fn list(&self, path: &SPath) -> Result<Vec<SPath>> {
        let mount = self.find_mount(path);
        mount.list(path).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{MountDef, SPath};
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::fs;
    use testdir::testdir;

    #[tokio::test]
    async fn test_store() {
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
        let store = super::MountsStorage::from(vec![mount1, mount2], &HashMap::new())
            .await
            .unwrap();

        store
            .write(&SPath::parse("/a.txt").unwrap(), vec![1])
            .await
            .unwrap();
        store
            .write(&SPath::parse("/foo/b.txt").unwrap(), vec![2])
            .await
            .unwrap();

        assert!(store
            .exists(&SPath::parse("/a.txt").unwrap())
            .await
            .unwrap());
        assert!(store
            .exists(&SPath::parse("/foo/b.txt").unwrap())
            .await
            .unwrap());

        assert_eq!(
            vec![1],
            store.read(&SPath::parse("/a.txt").unwrap()).await.unwrap()
        );
        assert_eq!(
            vec![2],
            store
                .read(&SPath::parse("/foo/b.txt").unwrap())
                .await
                .unwrap()
        );

        let got = store
            .read_stream(&SPath::parse("/foo/b.txt").unwrap())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(got, vec![bytes::Bytes::from(vec![2])]);

        assert!(store
            .list(&SPath::parse("/").unwrap())
            .await
            .unwrap()
            .contains(&SPath::parse("/a.txt").unwrap()));
        assert!(store
            .list(&SPath::parse("/foo").unwrap())
            .await
            .unwrap()
            .contains(&SPath::parse("/foo/b.txt").unwrap()));

        assert!(mount1_dir.join("a.txt").exists());
        assert!(mount2_dir.join("b.txt").exists());

        store
            .delete(&SPath::parse("/a.txt").unwrap())
            .await
            .unwrap();
        store
            .delete(&SPath::parse("/foo/b.txt").unwrap())
            .await
            .unwrap();

        assert!(!mount1_dir.join("a.txt").exists());
        assert!(!mount2_dir.join("b.txt").exists());
    }
}
