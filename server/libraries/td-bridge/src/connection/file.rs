//
// Copyright 2026 Tabsdata Inc.
//

//! The supervisor-mode connection store: one record per connection under the
//! instance repository, `connections/.store/<project_id>/<connection_id>.yaml`,
//! holding the full upsert triple and serving id-keyed fetch/delete.
//!
//! Writes are atomic (tmp + rename) with 0700/0600 perms. Secrets are
//! plaintext at rest, matching the local instance trust model (the embedded
//! database files in the same repository tree are equally readable);
//! instance-key encryption is a follow-up.

use super::{ConnectionStore, hydrated_of, merge_kept, read_of};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use td_common::env::{REPOSITORY_URI_ENV, get_current_dir};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::connection::{ConnectionHydrated, ConnectionRead, ConnectionUpsert};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId, ProjectId};
use tokio::fs;

pub const CONNECTIONS_FOLDER: &str = "connections";
const STORE_FOLDER: &str = ".store";

/// The on-disk record behind a connection: the effective triple, values
/// included.
#[derive(Serialize, Deserialize)]
struct ConnectionRecord {
    upsert: ConnectionUpsert,
}

pub struct FileConnectionStore {
    root: PathBuf,
}

impl FileConnectionStore {
    /// Roots at `$TD_URI_REPOSITORY/connections/`. Falls back to the current
    /// dir (dev/tests).
    pub fn new() -> Self {
        let root = match std::env::var(REPOSITORY_URI_ENV) {
            Ok(repository) if !repository.trim().is_empty() => PathBuf::from(repository),
            _ => get_current_dir(),
        };
        Self {
            root: root.join(CONNECTIONS_FOLDER),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_location(location: impl Into<PathBuf>) -> Self {
        Self {
            root: location.into().join(CONNECTIONS_FOLDER),
        }
    }

    fn record_path(&self, project_id: &ProjectId, connection_id: &ConnectionId) -> PathBuf {
        self.root
            .join(STORE_FOLDER)
            .join(project_id.to_string())
            .join(format!("{connection_id}.yaml"))
    }

    async fn read_record(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<Option<ConnectionRecord>, TdError> {
        let path = self.record_path(project_id, connection_id);
        let raw = match fs::read_to_string(&path).await {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(&path, &e)),
        };
        let record = serde_yaml::from_str(&raw).map_err(|e| {
            api_error!(
                ApiError::InternalError,
                "corrupt connection record '{}': {e}",
                path.display()
            )
        })?;
        Ok(Some(record))
    }
}

impl Default for FileConnectionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConnectionStore for FileConnectionStore {
    async fn upsert(
        &self,
        project_id: &ProjectId,
        _collection_id: &CollectionId,
        _collection: &CollectionName,
        connection_id: &ConnectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        if connection.connection.is_none() {
            return Err(api_error!(
                ApiError::InputError,
                "a connection upsert must include the dehydrated connection"
            ));
        }
        let previous = self.read_record(project_id, connection_id).await?;
        let record = ConnectionRecord {
            upsert: merge_kept(connection, previous.as_ref().map(|p| &p.upsert))?,
        };
        let payload = serde_yaml::to_string(&record)
            .map_err(|e| api_error!(ApiError::InternalError, "cannot serialize the record: {e}"))?;
        write_atomic(
            &self.record_path(project_id, connection_id),
            payload.as_bytes(),
        )
        .await?;
        Ok(read_of(&record.upsert))
    }

    async fn fetch(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionRead, TdError> {
        match self.read_record(project_id, connection_id).await? {
            Some(record) => Ok(read_of(&record.upsert)),
            None => Err(api_error!(
                ApiError::NotFound,
                "the collection has no connection configured"
            )),
        }
    }

    async fn fetch_hydrated(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionHydrated, TdError> {
        match self.read_record(project_id, connection_id).await? {
            Some(record) => Ok(hydrated_of(&record.upsert)),
            None => Err(api_error!(
                ApiError::NotFound,
                "the collection has no connection configured"
            )),
        }
    }

    async fn delete(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<(), TdError> {
        let path = self.record_path(project_id, connection_id);
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io_error(&path, &e)),
        }
        if let Some(dir) = path.parent() {
            let _ = fs::remove_dir(dir).await;
        }
        Ok(())
    }
}

async fn write_atomic(path: &Path, payload: &[u8]) -> Result<(), TdError> {
    let parent = path.parent().ok_or_else(|| {
        api_error!(
            ApiError::InternalError,
            "connection path '{}' has no parent",
            path.display()
        )
    })?;
    fs::create_dir_all(parent)
        .await
        .map_err(|e| io_error(parent, &e))?;
    restrict(parent, 0o700).await?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, payload)
        .await
        .map_err(|e| io_error(&tmp, &e))?;
    restrict(&tmp, 0o600).await?;
    fs::rename(&tmp, path)
        .await
        .map_err(|e| io_error(path, &e))?;
    Ok(())
}

#[cfg(unix)]
async fn restrict(path: &Path, mode: u32) -> Result<(), TdError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .await
        .map_err(|e| io_error(path, &e))
}

#[cfg(not(unix))]
async fn restrict(_path: &Path, _mode: u32) -> Result<(), TdError> {
    Ok(())
}

fn io_error(path: &Path, e: &std::io::Error) -> TdError {
    api_error!(
        ApiError::InternalError,
        "connection store IO error on '{}': {e}",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use td_objects::dxo::connection::KeyValue;

    fn store() -> FileConnectionStore {
        FileConnectionStore::with_location(td_common::tdtestdir!())
    }

    async fn upsert_conn(
        store: &FileConnectionStore,
        project: &ProjectId,
        connection_id: &ConnectionId,
        payload: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        let collection = CollectionName::try_from("col").unwrap();
        let collection_id = CollectionId::default();
        store
            .upsert(project, &collection_id, &collection, connection_id, payload)
            .await
    }

    fn upsert(name: &str, secret_value: Option<&str>) -> ConnectionUpsert {
        ConnectionUpsert {
            connection: Some(json!({
                "kind": "mysqlSrcConn",
                "name": name,
                "conn_configs": {"host": "h", "env:password": "CREDENTIALS_PASSWORD"},
            })),
            secrets: Some(vec![KeyValue {
                key: "CREDENTIALS_PASSWORD".to_string(),
                value: secret_value.map(str::to_string),
            }]),
        }
    }

    #[tokio::test]
    async fn test_upsert_fetch_roundtrip_names_only() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        let read = upsert_conn(&store, &project, &id, &upsert("Finance", Some("hunter2")))
            .await
            .unwrap();
        assert_eq!(read.secret_names, vec!["CREDENTIALS_PASSWORD"]);
        assert_eq!(read.connection["name"], "Finance");

        let read = store.fetch(&project, &id).await.unwrap();
        assert_eq!(read.secret_names, vec!["CREDENTIALS_PASSWORD"]);
        // Values never leave the store.
        assert!(!serde_json::to_string(&read).unwrap().contains("hunter2"));
    }

    #[tokio::test]
    async fn test_fetch_hydrated_returns_values() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("Finance", Some("hunter2")))
            .await
            .unwrap();

        let hydrated = store.fetch_hydrated(&project, &id).await.unwrap();
        assert_eq!(hydrated.connection["name"], "Finance");
        assert_eq!(hydrated.secrets.len(), 1);
        assert_eq!(hydrated.secrets[0].key, "CREDENTIALS_PASSWORD");
        assert_eq!(hydrated.secrets[0].value.as_deref(), Some("hunter2"));
        let read = store.fetch(&project, &id).await.unwrap();
        assert!(!serde_json::to_string(&read).unwrap().contains("hunter2"));
    }

    #[tokio::test]
    async fn test_fetch_hydrated_missing_is_not_found() {
        let store = store();
        let err = store
            .fetch_hydrated(&ProjectId::default(), &ConnectionId::default())
            .await
            .expect_err("gone");
        assert!(err.to_string().contains("no connection configured"));
    }

    #[tokio::test]
    async fn test_kept_value_merges_from_previous_record() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        // Rotation-style upsert: keep the stored secret value.
        upsert_conn(&store, &project, &id, &upsert("fin", None))
            .await
            .unwrap();

        let raw = std::fs::read_to_string(store.record_path(&project, &id)).unwrap();
        assert!(raw.contains("hunter2"), "kept value must be preserved");
    }

    #[tokio::test]
    async fn test_same_name_in_two_projects_does_not_collide() {
        let store = store();
        let (p1, p2) = (ProjectId::default(), ProjectId::default());
        let (id1, id2) = (ConnectionId::default(), ConnectionId::default());

        upsert_conn(&store, &p1, &id1, &upsert("fin", Some("one")))
            .await
            .unwrap();
        upsert_conn(&store, &p2, &id2, &upsert("fin", Some("two")))
            .await
            .unwrap();

        store.delete(&p1, &id1).await.unwrap();
        assert!(!store.record_path(&p1, &id1).exists());
        let hydrated = store.fetch_hydrated(&p2, &id2).await.unwrap();
        assert_eq!(hydrated.secrets[0].value.as_deref(), Some("two"));
    }

    #[tokio::test]
    async fn test_upsert_writes_only_the_id_keyed_record() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        let entries: Vec<_> = std::fs::read_dir(&store.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![STORE_FOLDER]);
    }

    #[tokio::test]
    async fn test_kept_value_without_previous_is_rejected() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        let err = upsert_conn(&store, &project, &id, &upsert("fin", None))
            .await
            .expect_err("keeping a never-stored value must fail");
        assert!(err.to_string().contains("not stored"));
    }

    #[tokio::test]
    async fn test_delete_is_idempotent_and_removes_the_record() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        store.delete(&project, &id).await.unwrap();
        assert!(!store.record_path(&project, &id).exists());

        // Deleting again (or a never-stored id) succeeds — contract.
        store.delete(&project, &id).await.unwrap();
        let err = store.fetch(&project, &id).await.expect_err("gone");
        assert!(err.to_string().contains("no connection configured"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_records_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        let mode = std::fs::metadata(store.record_path(&project, &id))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
