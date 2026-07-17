//
// Copyright 2026 Tabs Data Inc.
//

//! Shared fixtures for the connection service tests: mock stores and
//! request/collection builders.

use super::ConnectionUpsertService;
use crate::collection::service::create::CreateCollectionService;
use async_trait::async_trait;
use http::StatusCode;
use serde_json::json;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use ta_services::service::TdService;
use td_authz::AuthzContext;
use td_bridge::BridgeProviderHandle;
use td_bridge::connection::ConnectionStore;
use td_bridge::test::TestBridgeProvider;
use td_database::pool::TdPool;
use td_database::pool::scoped::{InProject, Scoped};
use td_error::{TdError, api_error};
use td_objects::Buildable;
use td_objects::dxo::collection::{CollectionCreate, CollectionDB};
use td_objects::dxo::connection::{ConnectionDB, ConnectionRead, ConnectionUpsert, KeyValue};
use td_objects::dxo::crudl::{CreateRequest, ReadRequest, RequestContext, UpdateRequest};
use td_objects::rest_urls::CollectionParam;
use td_objects::types::basic::{
    AccessTokenId, CollectionId, CollectionIdName, CollectionName, ConnectionId, Description,
    EntityId, GroupName, PermissionType, ProjectId, RoleName, UserEnabled, UserId, UserName,
};
use td_queries::LiveEntry;
use td_queries::test_utils::SeedEntry;
use td_tower::ctx_service::RawOneshot;

/// Records upserts; fetch/delete are unreachable — a test reaching them
/// panics, proving short-circuits.
#[derive(Default)]
pub struct MockStore {
    pub upserts: Arc<Mutex<Vec<(ProjectId, ConnectionId, ConnectionUpsert)>>>,
}

#[async_trait]
impl ConnectionStore for MockStore {
    async fn upsert(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
        _collection: &CollectionName,
        _collection_id: &CollectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        self.upserts
            .lock()
            .unwrap()
            .push((*project_id, *connection_id, connection.clone()));
        Ok(stored_read())
    }
    async fn fetch(&self, _: &ProjectId, _: &ConnectionId) -> Result<ConnectionRead, TdError> {
        unreachable!()
    }
    async fn delete(&self, _: &ProjectId, _: &ConnectionId) -> Result<(), TdError> {
        unreachable!()
    }
}

/// A store whose writes always fail — nothing may be persisted then.
/// `delete` succeeds: the create orchestrator's best-effort cleanup may call
/// it after a transaction failure.
pub struct FailingStore;

#[async_trait]
impl ConnectionStore for FailingStore {
    async fn upsert(
        &self,
        _: &ProjectId,
        _: &ConnectionId,
        _: &CollectionName,
        _: &CollectionId,
        _: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        Err(api_error!(
            status = StatusCode::BAD_GATEWAY,
            "store unreachable"
        ))
    }
    async fn fetch(&self, _: &ProjectId, _: &ConnectionId) -> Result<ConnectionRead, TdError> {
        unreachable!()
    }
    async fn delete(&self, _: &ProjectId, _: &ConnectionId) -> Result<(), TdError> {
        Ok(())
    }
}

/// Accepts upserts and serves fetches with a fixed set of stored secret
/// names — for the "keep the stored value" verification tests.
pub struct KeepStore {
    pub stored_secret_names: Vec<String>,
}

#[async_trait]
impl ConnectionStore for KeepStore {
    async fn upsert(
        &self,
        _: &ProjectId,
        _: &ConnectionId,
        _: &CollectionName,
        _: &CollectionId,
        _: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        Ok(stored_read())
    }
    async fn fetch(&self, _: &ProjectId, _: &ConnectionId) -> Result<ConnectionRead, TdError> {
        Ok(ConnectionRead::builder()
            .connection(json!({"kind": "mysqlSrcConn"}))
            .secret_names(self.stored_secret_names.clone())
            .var_names(Vec::<String>::new())
            .build()
            .unwrap())
    }
    async fn delete(&self, _: &ProjectId, _: &ConnectionId) -> Result<(), TdError> {
        unreachable!()
    }
}

fn stored_read() -> ConnectionRead {
    ConnectionRead::builder()
        .connection(json!({"kind": "mysqlSrcConn"}))
        .secret_names(vec!["CREDENTIALS_PASSWORD".to_string()])
        .var_names(Vec::<String>::new())
        .build()
        .unwrap()
}

pub fn bridge(store: impl ConnectionStore + 'static) -> Arc<BridgeProviderHandle> {
    Arc::new(BridgeProviderHandle::new(Arc::new(
        TestBridgeProvider::default().with_connection_store(store),
    )))
}

pub fn upsert_service(
    db: &TdPool,
    store: impl ConnectionStore + 'static,
) -> ConnectionUpsertService {
    ConnectionUpsertService::new(db.clone(), Arc::new(AuthzContext::default()), bridge(store))
}

/// A creation request; source/destination groups get the (mandatory)
/// connection payload attached, named after the collection.
pub fn create_request(
    project: ProjectId,
    name: &str,
    group: Option<GroupName>,
) -> Scoped<CreateRequest<(), CollectionCreate>, ProjectId> {
    let mut builder = CollectionCreate::builder();
    builder
        .name(CollectionName::try_from(name).unwrap())
        .description(Description::try_from("d").unwrap());
    if let Some(group) = group {
        let kind = if group == GroupName::destination() {
            "mysqlDestConn"
        } else {
            "mysqlSrcConn"
        };
        if group == GroupName::source() || group == GroupName::destination() {
            builder
                .connection(Some(document(name, kind)))
                .secrets(Some(secrets()))
                .vars(Some(vec![]));
        }
        builder.group(group);
    }
    RequestContext::with(AccessTokenId::default(), UserId::admin())
        .create((), builder.build().unwrap())
        .in_project(project)
}

pub async fn create_collection(
    db: &TdPool,
    project: ProjectId,
    name: &str,
    group: Option<GroupName>,
) {
    CreateCollectionService::with_defaults(db.clone())
        .service()
        .raw_oneshot(create_request(project, name, group))
        .await
        .unwrap();
}

pub async fn linked_connection(
    db: &TdPool,
    project: ProjectId,
    collection: &str,
) -> Option<ConnectionDB> {
    let collection: CollectionDB = db
        .scoped(project)
        .live()
        .fetch_one::<CollectionDB>(&CollectionName::try_from(collection).unwrap())
        .await
        .unwrap();
    match collection.connection_id {
        Some(id) => Some(
            db.scoped(project)
                .live()
                .fetch_one::<ConnectionDB>(&id)
                .await
                .unwrap(),
        ),
        None => None,
    }
}

pub fn collection_param(collection: &str) -> CollectionParam {
    CollectionParam::builder()
        .collection(CollectionIdName::from_str(collection).unwrap())
        .build()
        .unwrap()
}

fn context() -> RequestContext {
    RequestContext::with(AccessTokenId::default(), UserId::admin())
}

/// A user holding only `develop_project` on the given project: passes the
/// DevelopProject floor but fails the ManageProject bar that
/// connection-carrying (source/destination) operations require.
pub async fn seed_dev_user(db: &TdPool, project: ProjectId) -> UserId {
    let role = db
        .seed()
        .role(
            RoleName::try_from("test_dev_t").unwrap(),
            Description::try_from("develop_project on one project").unwrap(),
        )
        .await
        .unwrap();
    db.seed()
        .permission(
            PermissionType::try_from("develop_project").unwrap(),
            None,
            Some(EntityId::try_from(project).unwrap()),
            &role,
        )
        .await
        .unwrap();
    let user = db
        .seed()
        .user(
            &UserName::try_from("test_dev_t").unwrap(),
            &UserEnabled::from(true),
        )
        .await
        .unwrap();
    db.seed().user_role(&user.id, &role.id).await.unwrap();
    user.id
}

/// The canonical valid document: name = `name`, opaque `conn_configs`.
pub fn document(name: &str, conn_type: &str) -> serde_json::Value {
    json!({
        "kind": "connectionDef",
        "apiVersion": "1.0",
        "name": name,
        "type": conn_type,
        "conn_configs": {"host": "h", "env:password": "CREDENTIALS_PASSWORD"},
    })
}

pub fn secrets() -> Vec<KeyValue> {
    vec![KeyValue {
        key: "CREDENTIALS_PASSWORD".to_string(),
        value: Some("hunter2".to_string()),
    }]
}

pub fn upsert_request_body(
    project: ProjectId,
    collection: &str,
    body: ConnectionUpsert,
) -> Scoped<UpdateRequest<CollectionParam, ConnectionUpsert>, ProjectId> {
    context()
        .update(collection_param(collection), body)
        .in_project(project)
}

pub fn upsert_request_kind(
    project: ProjectId,
    collection: &str,
    kind: &str,
) -> Scoped<UpdateRequest<CollectionParam, ConnectionUpsert>, ProjectId> {
    let body = ConnectionUpsert::builder()
        .connection(Some(document(collection, kind)))
        .secrets(Some(secrets()))
        .vars(Some(vec![]))
        .build()
        .unwrap();
    upsert_request_body(project, collection, body)
}

pub fn upsert_request(
    project: ProjectId,
    collection: &str,
) -> Scoped<UpdateRequest<CollectionParam, ConnectionUpsert>, ProjectId> {
    upsert_request_kind(project, collection, "mysqlSrcConn")
}

pub fn read_request(
    project: ProjectId,
    collection: &str,
) -> Scoped<ReadRequest<CollectionParam>, ProjectId> {
    context()
        .read(collection_param(collection))
        .in_project(project)
}
