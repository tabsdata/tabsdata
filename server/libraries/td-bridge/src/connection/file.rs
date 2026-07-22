//
// Copyright 2026 Tabsdata Inc.
//

//! The supervisor-mode connection store: files under the instance
//! repository. Two artifacts per connection:
//!
//! - the authoritative record `connections/.store/<project_id>/<connection_id>.yaml`
//!   (the full upsert triple + the connection name) serving id-keyed
//!   fetch/delete;
//! - the worker artifact `connections/<project_id>/<name>.lower().json` — the
//!   dehydrated document verbatim, at the exact path locally-run function
//!   workers resolve `NamedConn(name)` references from (the worker takes its
//!   project from the request yaml; env refs are hydrated from the WORKER's
//!   process env at load, untouched here). Project-namespacing keeps
//!   same-named collections in different projects from colliding.
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

/// The on-disk record behind a connection: the name (worker-artifact key) and
/// the effective triple, values included.
#[derive(Serialize, Deserialize)]
struct ConnectionRecord {
    name: String,
    upsert: ConnectionUpsert,
}

pub struct FileConnectionStore {
    root: PathBuf,
}

impl FileConnectionStore {
    /// Roots at `$TD_URI_REPOSITORY/connections/` — the same env the function
    /// workers resolve their `NamedConn` lookups from, so both sides agree by
    /// construction. Falls back to the current dir (dev/tests).
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

    fn worker_path(&self, project_id: &ProjectId, name: &str) -> PathBuf {
        self.root
            .join(project_id.to_string())
            .join(format!("{}.json", name.to_lowercase()))
    }

    /// Whether a record OTHER than `connection_id`'s already holds `name` —
    /// the worker artifact path is derived from the name, so a second owner
    /// would hijack the first one's artifact.
    async fn name_owned_by_other(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
        name: &str,
    ) -> Result<bool, TdError> {
        let dir = self.root.join(STORE_FOLDER).join(project_id.to_string());
        let own = format!("{connection_id}.yaml");
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(io_error(&dir, &e)),
        };
        while let Some(entry) = entries.next_entry().await.map_err(|e| io_error(&dir, &e))? {
            if entry.file_name().to_string_lossy() == own {
                continue;
            }
            let raw = match fs::read_to_string(entry.path()).await {
                Ok(raw) => raw,
                Err(_) => continue, // not a record; best-effort scan
            };
            if let Ok(record) = serde_yaml::from_str::<ConnectionRecord>(&raw)
                && record.name == name
            {
                return Ok(true);
            }
        }
        Ok(false)
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
        connection_id: &ConnectionId,
        _collection: &CollectionName,
        _collection_id: &CollectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        let document = connection.connection.as_ref().ok_or_else(|| {
            api_error!(
                ApiError::InputError,
                "a connection upsert must include the dehydrated connection"
            )
        })?;
        let name = document
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| {
                api_error!(
                    ApiError::InputError,
                    "the dehydrated connection must carry a top-level 'name'"
                )
            })?
            .to_string();

        if self
            .name_owned_by_other(project_id, connection_id, &name)
            .await?
        {
            return Err(api_error!(
                ApiError::InputError,
                "the connection name '{name}' belongs to another collection's connection"
            ));
        }

        let previous = self.read_record(project_id, connection_id).await?;
        let record = ConnectionRecord {
            name: name.clone(),
            upsert: merge_kept(connection, previous.as_ref().map(|p| &p.upsert))?,
        };

        // A rename (the collection's name changed and the connection follows)
        // moves the worker artifact: drop the one under the previous name.
        if let Some(previous) = &previous
            && previous.name != record.name
        {
            let _ = fs::remove_file(self.worker_path(project_id, &previous.name)).await;
        }

        let payload = serde_yaml::to_string(&record)
            .map_err(|e| api_error!(ApiError::InternalError, "cannot serialize the record: {e}"))?;
        write_atomic(
            &self.record_path(project_id, connection_id),
            payload.as_bytes(),
        )
        .await?;
        let artifact = serde_json::to_string_pretty(document).map_err(|e| {
            api_error!(
                ApiError::InternalError,
                "cannot serialize the document: {e}"
            )
        })?;
        write_atomic(
            &self.worker_path(project_id, &record.name),
            artifact.as_bytes(),
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
        let Some(record) = self.read_record(project_id, connection_id).await? else {
            return Ok(()); // idempotent: deleting a never-stored id succeeds
        };
        let worker = self.worker_path(project_id, &record.name);
        if let Err(e) = fs::remove_file(&worker).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(io_error(&worker, &e));
        }
        let path = self.record_path(project_id, connection_id);
        if let Err(e) = fs::remove_file(&path).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(io_error(&path, &e));
        }
        // Best-effort: drop the per-project dirs once empty.
        for dir in [worker.parent(), path.parent()].into_iter().flatten() {
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
    use td_objects::Buildable;
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
            .upsert(project, connection_id, &collection, &collection_id, payload)
            .await
    }

    fn upsert(name: &str, secret_value: Option<&str>) -> ConnectionUpsert {
        ConnectionUpsert::builder()
            .connection(Some(json!({
                "kind": "mysqlSrcConn",
                "name": name,
                "conn_configs": {"host": "h", "env:password": "CREDENTIALS_PASSWORD"},
            })))
            .secrets(Some(vec![KeyValue {
                key: "CREDENTIALS_PASSWORD".to_string(),
                value: secret_value.map(str::to_string),
            }]))
            .vars(Some(vec![]))
            .build()
            .unwrap()
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
    async fn test_worker_artifact_is_the_document_at_the_lookup_path() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();
        let payload = upsert("Finance", Some("hunter2"));

        upsert_conn(&store, &project, &id, &payload).await.unwrap();

        // The exact path + content `NamedConn("Finance")` resolution reads —
        // project-namespaced, so same-named collections in other projects
        // cannot collide.
        let artifact = store.worker_path(&project, "Finance");
        assert!(artifact.ends_with(format!("{project}/finance.json")));
        let raw = std::fs::read_to_string(&artifact).unwrap();
        let document: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(document, payload.connection.unwrap());
        // The dehydrated document carries env refs, never values.
        assert!(!raw.contains("hunter2"));
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
        assert!(store.worker_path(&p1, "fin").exists());
        assert!(store.worker_path(&p2, "fin").exists());

        // Deleting one project's connection leaves the other's artifact.
        store.delete(&p1, &id1).await.unwrap();
        assert!(!store.worker_path(&p1, "fin").exists());
        assert!(store.worker_path(&p2, "fin").exists());
    }

    #[tokio::test]
    async fn test_name_owned_by_another_connection_is_rejected() {
        let store = store();
        let project = ProjectId::default();
        let (id1, id2) = (ConnectionId::default(), ConnectionId::default());

        upsert_conn(&store, &project, &id1, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        // A different connection taking the name would hijack id1's artifact.
        let err = upsert_conn(&store, &project, &id2, &upsert("fin", Some("x")))
            .await
            .expect_err("name is taken");
        assert!(err.to_string().contains("another collection"), "got: {err}");
        assert!(store.worker_path(&project, "fin").exists());

        // The owner itself can re-upsert and rename; a released name is free.
        upsert_conn(&store, &project, &id1, &upsert("fin2", None))
            .await
            .unwrap();
        upsert_conn(&store, &project, &id2, &upsert("fin", Some("x")))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_rename_moves_the_worker_artifact() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        // Same id, new document name — the collection was renamed.
        upsert_conn(&store, &project, &id, &upsert("fin2", None))
            .await
            .unwrap();

        assert!(!store.worker_path(&project, "fin").exists());
        let raw = std::fs::read_to_string(store.worker_path(&project, "fin2")).unwrap();
        let document: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(document["name"], "fin2");
        // Kept values survive the rename.
        let record = std::fs::read_to_string(store.record_path(&project, &id)).unwrap();
        assert!(record.contains("hunter2"));
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
    async fn test_delete_is_idempotent_and_removes_both_artifacts() {
        let store = store();
        let project = ProjectId::default();
        let id = ConnectionId::default();

        upsert_conn(&store, &project, &id, &upsert("fin", Some("hunter2")))
            .await
            .unwrap();
        store.delete(&project, &id).await.unwrap();
        assert!(!store.worker_path(&project, "fin").exists());
        assert!(!store.record_path(&project, &id).exists());

        // Deleting again (or a never-stored id) succeeds — contract.
        store.delete(&project, &id).await.unwrap();
        let err = store.fetch(&project, &id).await.expect_err("gone");
        assert!(err.to_string().contains("no connection configured"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_artifacts_are_private() {
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
