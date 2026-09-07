//
// Copyright 2026 Tabsdata Inc.
//

//! Update a source/destination collection's connection. ManageProject-gated
//! ("CRUD sources and destinations" in the permission model). The connection
//! exists by construction — it was created WITH the collection (1:1) — so
//! this is a pure store update: validate
//! against the existing row (kind immutable, name = the collection's), then
//! write the store. No DB write happens at all, and no DB connection is held
//! across the store's network call (the resolve phase has its own).

use super::{
    ConnectionResolution, ensure_connectable, resolve_connection, validate_document,
    validate_upsert,
};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_bridge::BridgeProviderHandle;
use td_bridge::connection::ConnectionStore;
use td_database::access::{DbPool, InProject};
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert};
use td_objects::dxo::crudl::{UpdateRequest, WithinProject};
use td_objects::types::basic::ProjectId;
use td_queries::Queries;

#[service_factory]
async fn connection_upsert(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<UpdateRequest<ConnectionUpsert, CollectionParam>>,
) -> Result<ConnectionRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, payload) = request.into_parts();

    // Resolve on a short connection of its own.
    let resolution = {
        let mut conn = db.read().await?;
        let grant = authz
            .require::<Project, ManageProject>(&mut conn, &ctx, &project)
            .await?;
        let conn = conn.get(grant);
        let collection: ActiveCollectionDB = conn.fetch_one(&param.collection).await?;
        ensure_connectable(&collection)?;
        validate_upsert(&payload)?;
        let document = validate_document(&payload)?;
        resolve_connection(conn, &collection, &document).await?
    };

    // The store write, with no DB connection held.
    let store = bridge.connection_store();
    verify_kept_keys(store, &project, &resolution).await?;
    let mut read = store
        .upsert(
            &project,
            &resolution.row.base.id,
            &resolution.collection,
            &resolution.collection_id,
            &payload,
        )
        .await?;
    read.id = Some(resolution.row.base.id);
    Ok(read)
}

/// Keys carried with `value: None` mean "keep the stored value" — only usable
/// if the store actually holds them, otherwise hydration would come up empty.
async fn verify_kept_keys(
    store: &dyn ConnectionStore,
    project: &ProjectId,
    resolution: &ConnectionResolution,
) -> Result<(), TdError> {
    let document = &resolution.document;
    if document.kept_secrets.is_empty() && document.kept_vars.is_empty() {
        return Ok(());
    }
    let stored = store.fetch(project, &resolution.row.base.id).await?;
    let missing: Vec<&String> = document
        .kept_secrets
        .iter()
        .filter(|k| !stored.secret_names.contains(k))
        .chain(
            document
                .kept_vars
                .iter()
                .filter(|k| !stored.var_names.contains(k)),
        )
        .collect();
    if !missing.is_empty() {
        return Err(api_error!(
            ApiError::InputError,
            "secrets/vars marked 'keep the stored value' are not in the store: {}",
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::services::connection::test_support::{
        KeepStore, MockStore, collection_param, create_collection, document, linked_connection,
        read_request, secrets, seed_dev_user, upsert_request, upsert_request_body,
        upsert_request_kind, upsert_service,
    };
    use std::sync::{Arc, Mutex};
    use td_bridge::connection::noop::NoopConnectionStore;
    use td_bridge::test::TestBridgeProvider;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_objects::dxo::connection::KeyValue;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, GroupName, UserEnabled, UserName};
    use td_queries::test_utils::SeedEntry;

    // The orchestrator's metadata is its resolve pipeline (the store phase
    // is hand-rolled around it).

    #[td_test::test(db)]
    async fn test_upsert_forwards_to_store(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src", Some(GroupName::source())).await;
        let upserts = Arc::new(Mutex::new(Vec::new()));

        let response = upsert_service(
            &db,
            MockStore {
                upserts: upserts.clone(),
            },
        )
        .connection_upsert(upsert_request(p.id, "src"))
        .await
        .unwrap();

        let row = linked_connection(&db, p.id, "src").await.unwrap();
        let recorded = upserts.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, p.id);
        assert_eq!(
            recorded[0].1, row.base.id,
            "store keyed by the row minted at create"
        );
        assert_eq!(
            response.secret_names,
            vec!["CREDENTIALS_PASSWORD".to_string()]
        );
        assert_eq!(response.id, Some(row.base.id));
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_datafactory(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "fac", None).await; // default group -> DataFactory
        let result = upsert_service(&db, MockStore::default())
            .connection_upsert(upsert_request(p.id, "fac"))
            .await;
        assert!(result.is_err(), "DataFactory must be rejected");
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_missing_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src3", Some(GroupName::source())).await;
        // Secrets without the dehydrated connection — rejected (no subset
        // updates until fetch-and-align lands).
        let body = ConnectionUpsert {
            connection: None,
            secrets: Some(secrets()),
            vars: None,
        };
        let result = upsert_service(&db, MockStore::default())
            .connection_upsert(upsert_request_body(p.id, "src3", body))
            .await;
        assert!(result.is_err(), "connection-less upsert must be rejected");
    }

    #[td_test::test(db)]
    async fn test_upsert_requires_project_role(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "src2", Some(GroupName::source())).await;
        // A user with no role in the project fails the DevelopProject check.
        let outsider = db
            .seed()
            .user(&UserName::try_from("outsider").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap();
        let body = ConnectionUpsert {
            connection: Some(document("src2", "mysqlSrcConn")),
            secrets: Some(secrets()),
            vars: Some(vec![]),
        };
        let request = RequestContext::with(AccessTokenId::default(), outsider.id)
            .update(collection_param("src2"), body)
            .in_project(p.id);
        let result = upsert_service(&db, MockStore::default())
            .connection_upsert(request)
            .await;
        assert!(
            result.is_err(),
            "a user without a project role must be rejected"
        );
    }

    #[td_test::test(db)]
    async fn test_upsert_requires_manage_project(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcm", Some(GroupName::source())).await;
        let dev = seed_dev_user(&db, p.id).await;
        let body = ConnectionUpsert {
            connection: Some(document("srcm", "mysqlSrcConn")),
            secrets: Some(secrets()),
            vars: Some(vec![]),
        };
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .update(collection_param("srcm"), body)
            .in_project(p.id);
        let err = upsert_service(&db, MockStore::default())
            .connection_upsert(request)
            .await
            .expect_err("develop_project must not suffice for a connection upsert");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }

    #[td_test::test(db)]
    async fn test_read_requires_manage_project(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcr", Some(GroupName::source())).await;
        let dev = seed_dev_user(&db, p.id).await;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .read(collection_param("srcr"))
            .in_project(p.id);
        let err = Services::with_defaults(db.clone())
            .connection_read(request)
            .await
            .expect_err("develop_project must not suffice for a connection read");
        assert_eq!(err.api_error(), ApiError::Forbidden);
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_kind_change(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srck", Some(GroupName::source())).await; // mysqlSrcConn
        let result = upsert_service(&db, MockStore::default())
            .connection_upsert(upsert_request_kind(p.id, "srck", "postgresSrcConn"))
            .await;
        let err = result.expect_err("connector kind must be immutable");
        assert!(err.to_string().contains("cannot change"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_upsert_rejects_duplicate_keys(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcd", Some(GroupName::source())).await;
        // Same key twice (here once in secrets, once in vars) — which value
        // applies would be backend-dependent.
        let body = ConnectionUpsert {
            connection: Some(document("srcd", "mysqlSrcConn")),
            secrets: Some(secrets()),
            vars: Some(vec![KeyValue {
                key: "CREDENTIALS_PASSWORD".to_string(),
                value: Some("other".to_string()),
            }]),
        };
        let err = upsert_service(&db, MockStore::default())
            .connection_upsert(upsert_request_body(p.id, "srcd", body))
            .await
            .expect_err("duplicate keys must be rejected");
        assert!(err.to_string().contains("duplicate"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_upsert_kept_value_checked_against_store(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcv", Some(GroupName::source())).await;
        let service = upsert_service(
            &db,
            KeepStore {
                stored_secret_names: vec!["CREDENTIALS_PASSWORD".to_string()],
            },
        );

        // Keeping a stored secret is fine.
        let keep_stored = ConnectionUpsert {
            connection: Some(document("srcv", "mysqlSrcConn")),
            secrets: Some(vec![KeyValue {
                key: "CREDENTIALS_PASSWORD".to_string(),
                value: None,
            }]),
            vars: Some(vec![]),
        };
        service
            .connection_upsert(upsert_request_body(p.id, "srcv", keep_stored))
            .await
            .expect("keeping a stored secret must be accepted");

        // Keeping a never-stored secret would hydrate to nothing.
        let keep_missing = ConnectionUpsert {
            connection: Some(document("srcv", "mysqlSrcConn")),
            secrets: Some(vec![
                KeyValue {
                    key: "CREDENTIALS_PASSWORD".to_string(),
                    value: Some("hunter2".to_string()),
                },
                KeyValue {
                    key: "NEVER_STORED".to_string(),
                    value: None,
                },
            ]),
            vars: Some(vec![]),
        };
        let err = service
            .connection_upsert(upsert_request_body(p.id, "srcv", keep_missing))
            .await
            .expect_err("keeping a never-stored secret must be rejected");
        assert!(err.to_string().contains("NEVER_STORED"), "got: {err}");
    }

    #[td_test::test(db)]
    async fn test_upsert_with_unconfigured_store_fails(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        create_collection(&db, p.id, "srcq", Some(GroupName::source())).await;
        let err = upsert_service(&db, NoopConnectionStore)
            .connection_upsert(upsert_request(p.id, "srcq"))
            .await
            .expect_err("unconfigured store must error");
        assert!(err.to_string().contains("not configured"));
    }

    /// The "supervisor mode works" pin: collection creation and the
    /// upsert/read pipelines against the real repository-file store, worker
    /// artifact included.
    #[td_test::test(db)]
    async fn test_supervisor_file_store_end_to_end(db: TdPool) {
        use crate::collection::services::connection::test_support::create_request;
        use td_bridge::connection::file::FileConnectionStore;

        let p = db.seed().test_project().await.unwrap();
        let dir = td_common::tdtestdir!();
        let bridge = || {
            Ctx::new(BridgeProviderHandle::new(Arc::new(
                TestBridgeProvider::default()
                    .with_connection_store(FileConnectionStore::with_location(dir.clone())),
            )))
        };

        Services {
            bridge: bridge(),
            ..Services::with_defaults(db.clone())
        }
        .create_collection(create_request(p.id, "srcw", Some(GroupName::source())))
        .await
        .expect("supervisor-mode create must work");

        // The worker artifact is in place for `NamedConn("srcw")` resolution,
        // under the run's project.
        assert!(
            dir.join("connections")
                .join(p.id.to_string())
                .join("srcw.json")
                .is_file()
        );

        // The update pipeline overwrites it...
        let response = Services {
            bridge: bridge(),
            ..Services::with_defaults(db.clone())
        }
        .connection_upsert(upsert_request(p.id, "srcw"))
        .await
        .expect("supervisor-mode upsert must work");
        assert_eq!(response.secret_names, vec!["CREDENTIALS_PASSWORD"]);

        // ...and the read pipeline serves it back, names only.
        let read = Services {
            bridge: bridge(),
            ..Services::with_defaults(db.clone())
        }
        .connection_read(read_request(p.id, "srcw"))
        .await
        .expect("supervisor-mode read must work");
        assert_eq!(read.secret_names, vec!["CREDENTIALS_PASSWORD"]);
        assert!(!serde_json::to_string(&read).unwrap().contains("hunter2"));
    }
}
