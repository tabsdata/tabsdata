//
// Copyright 2024 Tabsdata Inc.
//

//! Update a collection. Renaming a source/destination collection renames its
//! connection too (1:1 — the collection's name IS the connection's name), so
//! the orchestration mirrors create, store-first:
//!
//! 1. resolve (read-only): authz, validate, build the update row and detect
//!    the rename — nothing persisted.
//! 2. store rename (rename only): rewrite the stored document's `name`, which
//!    also moves the worker artifact. No DB connection held across it.
//! 3. update (short TX): apply the row update. If this fails, the store is
//!    put back under the old name best-effort and the error surfaces.
//!
//! `collection_type` is immutable: it is inferred from the group at creation
//! and never accepted on update. `CollectionUpdate` carries no group/type
//! field and `CollectionUpdateDB` writes only name/description/modified_*, so
//! the type is preserved for free. Any future "move collection to group"
//! feature must reject the change when the collection_type is
//! Source/Destination.

use crate::collection::CollectionError;
use crate::collection::services::connection::ensure_collection_name_free;
use crate::collection::services::connection::{escalate_for_connected, rename_stored_connection};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::SeesProjectRows;
use td_database::access::{AccessMode, DbPool, InProject, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::collection::ActiveCollectionDBWithNames;
use td_objects::dxo::collection::CollectionParam;
use td_objects::dxo::collection::{
    ActiveCollectionDB, CollectionRead, CollectionUpdate, CollectionUpdateDB,
};
use td_objects::dxo::crudl::{UpdateRequest, WithinProject};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId};
use td_queries::Queries;
use tracing::warn;

#[service_factory]
async fn update_collection(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<UpdateRequest<CollectionUpdate, CollectionParam>>,
) -> Result<CollectionRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, update) = request.into_parts();

    // 1. resolve (read-only): authz, validate, build the update row, detect rename.
    let (plan, grant) = {
        let mut conn = db.read().await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut conn, &ctx, &project)
            .await?;
        let conn = conn.get(grant);
        let collection: ActiveCollectionDB = conn.fetch_one(&param.collection).await?;
        // source/destination collections carry connection config: manage_project
        escalate_for_connected(&authz, conn, &ctx, &project, &collection.collection_type).await?;
        update_collection_validate(&update)?;
        let mut update_db = CollectionUpdateDB::from(&collection);
        update_db.modified_on = ctx.time.into();
        update_db.modified_by_id = ctx.user_id.into();
        update.apply_to(&mut update_db);
        let plan = plan_collection_update(conn, &collection, &update, &update_db).await?;
        (plan, grant)
    };

    // 2. store rename (rename only).
    let store = bridge.connection_store();
    if let Some(rename) = &plan.rename {
        rename_stored_connection(
            store,
            &project,
            &rename.connection_id,
            &rename.collection_id,
            &rename.new_name,
        )
        .await?;
    }

    // 3. update (short TX).
    let updated = async {
        let mut trx = db.begin().await?;
        let read = apply_collection_update(trx.get(grant), &plan).await?;
        trx.commit().await?;
        Ok::<_, TdError>(read)
    }
    .await;
    match updated {
        Ok(read) => Ok(read),
        Err(e) => {
            // The DB still references the old name — put the store back
            // best-effort (idempotent; a retry of either direction overwrites).
            if let Some(rename) = &plan.rename
                && let Err(revert) = rename_stored_connection(
                    store,
                    &project,
                    &rename.connection_id,
                    &rename.collection_id,
                    &rename.old_name,
                )
                .await
            {
                warn!(
                    error = %revert,
                    "could not revert the connection store rename; \
                     upsert the connection to repair"
                );
            }
            Err(e)
        }
    }
}

pub fn update_collection_validate(update: &CollectionUpdate) -> Result<(), TdError> {
    if update.name.is_none() && update.description.is_none() && update.base_path.is_none() {
        Err(CollectionError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

/// Everything the update transaction persists, resolved up front, plus the
/// store rename the orchestrator must perform before it (a
/// source/destination collection's name is its connection's name — the
/// connection follows the rename).
#[derive(Debug, Clone)]
pub struct CollectionUpdatePlan {
    pub collection_id: CollectionId,
    pub update: CollectionUpdateDB,
    pub rename: Option<ConnectionRename>,
}

/// A rename of a connected collection: the stored document's `name` and the
/// worker artifact must move with it. `old_name` is kept so a failed
/// transaction can put the store back.
#[derive(Debug, Clone)]
pub struct ConnectionRename {
    pub connection_id: ConnectionId,
    pub collection_id: CollectionId,
    pub old_name: CollectionName,
    pub new_name: CollectionName,
}

pub async fn plan_collection_update<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    collection: &ActiveCollectionDB,
    update: &CollectionUpdate,
    update_db: &CollectionUpdateDB,
) -> Result<CollectionUpdatePlan, TdError> {
    // A rename to a name already taken is rejected up front with a clean
    // "already exists" error for every collection flavour (not just connected),
    // instead of the raw unique-constraint violation from the update write.
    // Connected collections additionally need this before the store rename:
    // otherwise the existing owner's worker artifact would be overwritten (and
    // deleted by the revert). Renaming to the current name is a no-op.
    if let Some(name) = &update.name
        && *name != collection.name
    {
        ensure_collection_name_free(conn, name).await?;
    }
    let rename = match (&update.name, collection.connection_id) {
        (Some(name), Some(connection_id)) if *name != collection.name => Some(ConnectionRename {
            connection_id,
            collection_id: collection.id,
            old_name: collection.name.clone(),
            new_name: name.clone(),
        }),
        _ => None,
    };
    Ok(CollectionUpdatePlan {
        collection_id: collection.id,
        update: update_db.clone(),
        rename,
    })
}

/// The update transaction: apply the (store-confirmed) update and read the
/// result back.
pub async fn apply_collection_update<S: SeesProjectRows, M: WriteMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    plan: &CollectionUpdatePlan,
) -> Result<CollectionRead, TdError> {
    conn.update::<CollectionUpdateDB>(&plan.update, plan.collection_id)
        .await?;
    let with_names: ActiveCollectionDBWithNames = conn.fetch_one(plan.collection_id).await?;
    CollectionRead::from_dao(with_names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::collection::CollectionError;
    use crate::collection::services::connection::test_support::{
        bridge, create_collection, create_request, linked_connection, seed_dev_user,
    };
    use axum::body::Body;
    use axum::extract::Request;
    use std::str::FromStr;
    use td_bridge::connection::memory::InMemoryConnectionStore;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{ApiError, TdError, assert_service_error};
    use td_objects::dxo::bundle::BundleDB;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionVersionParam;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::function_upload::FunctionUpload;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BasePath, CollectionName, Decorator, Description, FunctionName,
        FunctionRuntimeValues, FunctionStatus, GroupName, ProjectId, UserId, UserName,
    };
    use td_objects::types::basic::{CollectionIdName, ReuseFrozen};
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    // The orchestrator's metadata is its resolve pipeline (the store/tx
    // phases are hand-rolled around it).

    fn update_request(
        project: ProjectId,
        collection: &str,
        update: CollectionUpdate,
    ) -> WithinProject<UpdateRequest<CollectionUpdate, CollectionParam>> {
        RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection).unwrap(),
                },
                update,
            )
            .in_project(project)
    }

    fn rename_to(name: &str) -> CollectionUpdate {
        CollectionUpdate {
            name: Some(CollectionName::try_from(name).unwrap()),
            description: None,
            base_path: None,
        }
    }

    #[td_test::test(db)]
    async fn test_update_collection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let create_name = CollectionName::try_from("ds0").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&create_name, &UserId::admin())
            .await;

        let before_update = AtTime::now();

        let name = CollectionName::try_from("ds1").unwrap();
        let description = Description::try_from("DS1").unwrap();

        let update = CollectionUpdate {
            name: Some(name.clone()),
            description: Some(description.clone()),
            base_path: None,
        };

        let service = Services::with_defaults(db);
        let response = service
            .update_collection(update_request(p.id, &create_name.to_string(), update))
            .await;
        assert!(response.is_ok());
        let updated = response.unwrap().data;

        assert_eq!(updated.name, name);
        assert_eq!(updated.description, description);
        assert!(*updated.created_on < before_update);
        assert_eq!(*updated.created_by_id, UserId::admin());
        assert_eq!(*updated.created_by, UserName::admin());
        assert!(*updated.modified_on > before_update);
        assert_eq!(*updated.modified_by_id, UserId::admin());
        assert_eq!(*updated.modified_by, UserName::admin());
    }

    async fn update_collection(
        db: &TdPool,
        project: ProjectId,
        collection_name: &CollectionName,
        name: Option<CollectionName>,
        description: Option<Description>,
        base_path: Option<Option<BasePath>>,
    ) -> Result<CollectionRead, TdError> {
        let update = CollectionUpdate {
            name,
            description,
            base_path,
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                update,
            )
            .in_project(project);
        Services::with_defaults(db.clone())
            .update_collection(request)
            .await
            .map(|response| response.data)
    }

    /// The current Active version of the function named `name`.
    async fn active_function(db: &TdPool, name: &FunctionName) -> Result<FunctionDB, TdError> {
        let active: Vec<FunctionDB> = db
            .timeline()
            .fetch_all::<FunctionDB>(&FunctionStatus::Active)
            .await?;
        let function = active
            .into_iter()
            .find(|f| f.base.name == *name)
            .expect("matching active function exists");
        Ok(function)
    }

    #[td_test::test(db)]
    async fn test_update_collection_base_path(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("bp_coll")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        // Seeded collections start with NULL base_path (they inherit the project).
        assert_eq!(collection.base_path, None);

        let data = BasePath::try_from("/data")?;
        update_collection(
            &db,
            p.id,
            &collection_name,
            None,
            None,
            Some(Some(data.clone())),
        )
        .await?;

        let reloaded: ActiveCollectionDB =
            db.fetch_one::<ActiveCollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, Some(data.clone()));

        // Clearing it (base_path = null) reverts to inheriting the project.
        update_collection(&db, p.id, &collection_name, None, None, Some(None)).await?;
        let reloaded: ActiveCollectionDB =
            db.fetch_one::<ActiveCollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, None, "cleared back to inherit");
        Ok(())
    }

    /// The update DTO's `base_path` is a JSON tri-state: absent leaves it
    /// unchanged, an explicit `null` clears it (inherit the project), a value
    /// sets a concrete prefix.
    #[test]
    fn test_collection_update_base_path_tristate_deser() {
        let unchanged: CollectionUpdate = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(unchanged.base_path, None, "absent -> unchanged");

        let cleared: CollectionUpdate = serde_json::from_str(r#"{"base_path":null}"#).unwrap();
        assert_eq!(cleared.base_path, Some(None), "null -> clear (inherit)");

        let set: CollectionUpdate = serde_json::from_str(r#"{"base_path":"/x"}"#).unwrap();
        assert_eq!(
            set.base_path,
            Some(Some(BasePath::try_from("/x").unwrap())),
            "value -> set concrete prefix"
        );
    }

    #[td_test::test(db)]
    async fn test_update_collection_nothing_to_update_errors(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let collection_name = CollectionName::try_from("noop_coll").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await;

        // name=None, description=None, base_path omitted (None) -> nothing to update.
        let update = CollectionUpdate {
            name: None,
            description: None,
            base_path: None,
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str()).unwrap(),
                },
                update,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db);
        assert_service_error(service.update_collection(request).await, |err| {
            #[allow(unreachable_patterns)]
            match err {
                CollectionError::UpdateRequestHasNothingToUpdate => {}
                other => panic!("Expected 'UpdateRequestHasNothingToUpdate', got {other:?}"),
            }
        });
    }

    /// A collection base_path change must not relocate artifacts that were
    /// generated under the old base_path: those rows carry their own stamped
    /// prefix so they stay findable. The collection update bumps no function
    /// version, so the function row is untouched and the bundle stays put.
    #[td_test::test(db)]
    async fn test_collection_base_path_update_does_not_relocate_existing_function_artifacts(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("move_coll")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;
        let context = Services::with_defaults(db.clone());

        // Put the collection on base_path "/a" before any artifacts exist.
        let a = BasePath::try_from("/a")?;
        update_collection(
            &db,
            p.id,
            &collection_name,
            None,
            None,
            Some(Some(a.clone())),
        )
        .await?;

        // Upload a real bundle so a Pending BundleDB row + storage object exist.
        let payload = b"BUNDLE-PAYLOAD".to_vec();
        let upload_request = Request::builder()
            .body(Body::from(payload.clone()))
            .unwrap();
        let upload = FunctionUpload::new(upload_request);
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                upload,
            )
            .in_project(p.id);
        let bundle = context.clone().upload_function(request).await?.data;

        let create = FunctionRegister {
            child_functions: None,
            name: "artifact_fn".try_into()?,
            description: "description".try_into()?,
            bundle_id: bundle.id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(request)
            .await?;

        // Both artifacts are stamped with the old "/a" prefix at create time.
        let function_name = FunctionName::try_from("artifact_fn")?;
        let function = active_function(&db, &function_name).await?;
        assert_eq!(function.base.stage_base_path, a);
        let bundle_before: Vec<BundleDB> =
            db.fetch_all::<BundleDB>(&function.base.bundle_id).await?;
        assert_eq!(bundle_before.len(), 1);
        assert_eq!(bundle_before[0].base.base_path, a);

        let b = BasePath::try_from("/b")?;
        update_collection(
            &db,
            p.id,
            &collection_name,
            None,
            None,
            Some(Some(b.clone())),
        )
        .await?;
        let reloaded: ActiveCollectionDB =
            db.fetch_one::<ActiveCollectionDB>(&collection.id).await?;
        assert_eq!(reloaded.base_path, Some(b));

        // The previously-generated artifacts still carry "/a": the move did not
        // relocate them, and no new function version was created.
        let function_after = active_function(&db, &function_name).await?;
        assert_eq!(
            function_after.base.id, function.base.id,
            "collection update must not bump the function version"
        );
        assert_eq!(
            function_after.base.stage_base_path, a,
            "function stage_base_path stays on the prefix it was stamped with"
        );
        let bundle_after: Vec<BundleDB> = db
            .fetch_all::<BundleDB>(&function_after.base.bundle_id)
            .await?;
        assert_eq!(bundle_after.len(), 1);
        assert_eq!(
            bundle_after[0].base.base_path, a,
            "bundle base_path stays on the prefix it was stamped with"
        );

        // And the bundle is still downloadable: the download layer reads the
        // stamped bundle.base_path ("/a"), unaffected by the collection move.
        use bytes::Bytes;
        use futures::StreamExt;
        let download_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .read(FunctionVersionParam {
                function_version: function_after.base.id.clone(),
            });
        let response = context
            .clone()
            .download_bundle(download_request)
            .await?
            .data;
        assert_eq!(response.etag, bundle_after[0].base.hash.clone());
        let mut stream = response.stream.into_inner();
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        assert_eq!(
            Bytes::from(buf).as_ref(),
            payload.as_slice(),
            "bundle still streams after the collection moved"
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_rename_connected_collection_renames_the_connection(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .create_collection(create_request(p.id, "src0", Some(GroupName::source())))
        .await
        .unwrap();

        let renamed = Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .update_collection(update_request(p.id, "src0", rename_to("src1")))
        .await
        .unwrap();
        assert_eq!(renamed.name.to_string(), "src1");

        // The stored document followed the rename, values intact.
        let connection = linked_connection(&db, p.id, "src1").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.base.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "src1");
        assert_eq!(stored.secret_names, vec!["CREDENTIALS_PASSWORD"]);
    }

    #[td_test::test(db)]
    async fn test_failed_rename_puts_the_store_back(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let bridge = bridge(InMemoryConnectionStore::default());

        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .create_collection(create_request(p.id, "src0", Some(GroupName::source())))
        .await
        .unwrap();
        let taken = CollectionName::try_from("taken").unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .collection(&taken, &UserId::admin())
            .await;

        // The resolve pre-check rejects the taken name before the store is
        // ever touched (the orchestrator's revert remains as the fallback
        // for the create/rename race the pre-check cannot see).
        Services {
            bridge: bridge.clone(),
            ..Services::with_defaults(db.clone())
        }
        .update_collection(update_request(p.id, "src0", rename_to("taken")))
        .await
        .expect_err("renaming onto an existing collection must fail");

        let connection = linked_connection(&db, p.id, "src0").await.unwrap();
        let stored = bridge
            .connection_store()
            .fetch(&p.id, &connection.base.id)
            .await
            .unwrap();
        assert_eq!(stored.connection["name"], "src0");
    }
    /// Renaming re-keys a source collection's connection document, so it
    /// requires ManageProject; plain collections need only DevelopProject.
    #[td_test::test(db)]
    async fn test_source_update_requires_manage_project(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let dev = seed_dev_user(&db, p.id).await;

        // DataFactory rename with develop_project: fine.
        Services::with_defaults(db.clone())
            .create_collection(create_request(p.id, "dev_df_u", None))
            .await?;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .update(
                CollectionParam {
                    collection: CollectionIdName::from_str("dev_df_u")?,
                },
                rename_to("dev_df_u2"),
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .update_collection(request)
            .await?;

        // Source rename with develop_project: forbidden.
        create_collection(&db, p.id, "dev_src_u", Some(GroupName::source())).await;
        let request = RequestContext::with(AccessTokenId::default(), dev)
            .update(
                CollectionParam {
                    collection: CollectionIdName::from_str("dev_src_u")?,
                },
                rename_to("dev_src_u2"),
            )
            .in_project(p.id);
        let err = Services::with_defaults(db.clone())
            .update_collection(request)
            .await
            .expect_err("develop_project must not suffice to rename a source collection");
        assert_eq!(err.api_error(), ApiError::Forbidden);
        Ok(())
    }
}
