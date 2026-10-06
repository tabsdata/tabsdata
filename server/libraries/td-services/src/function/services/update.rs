//
// Copyright 2025 Tabsdata Inc.
//

use crate::function::ensure_user_managed;
use crate::function::lifecycle::{reconcile_dq_summary_trend_children, update_one};
use crate::function::register::ensure_no_dq_summary_trends_table_types;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::SeesProjectRows;
use td_database::access::{AccessMode, DbPool, InProject, retrying};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::collection::{ActiveCollectionDB, CollectionDB, CollectionParam};
use td_objects::dxo::crudl::{UpdateRequest, WithinProject};
use td_objects::dxo::function::FunctionParam;
use td_objects::dxo::function::{Function, FunctionDBWithNames, FunctionUpdate};
use td_objects::types::basic::CollectionId;
use td_objects::{FromDao, and};
use td_queries::Queries;

#[service_factory]
async fn update_function(
    authz: Ctx<AuthzContext>,
    scheduler_notify: Ctx<SchedulerNotify>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<UpdateRequest<FunctionUpdate, (CollectionParam, FunctionParam)>>,
) -> Result<Function, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, (collection_param, function_param), update) = request.into_parts();
    let read = retrying(|| async {
        let mut gated_trx = db.begin_exclusive().await?;
        let grant = authz
            .require::<Project, DevelopProject>(&mut gated_trx, &ctx, &project)
            .await?;
        let conn = gated_trx.get(grant);

        // Bare-table row lock (views with outer joins aren't lockable); the
        // active-view fetch is the liveness check, decided under the lock.
        let (locked, at) = conn
            .for_update()
            .serialize_on::<CollectionDB>(&collection_param.collection)
            .await?;
        let collection: ActiveCollectionDB = conn.fetch_one(&locked.base.id).await?;

        // Before-update ids: the function id does not change, the function
        // version id does.
        let current: FunctionDBWithNames = conn
            .scd2()
            .fetch_one(and!(&collection_param.collection, &function_param.function))
            .await?;

        ensure_user_managed(&current.base.base.name, &current.base.base.function_type)?;
        ensure_no_dq_summary_trends_table_types(&update.table_types)?;
        let function = update_one(conn, &ctx, &bridge, &collection, &current, &update, None, at).await?;
        reconcile_dq_summary_trend_children(
            conn,
            &ctx,
            &bridge,
            &collection,
            &function,
            &update.child_functions,
            update.reuse_frozen_tables,
            at,
        )
        .await?;

        let with_names: FunctionDBWithNames = conn.timeline().fetch_one(&function.id).await?;
        let read = Function::from_dao(with_names)?;
        gated_trx.commit().await?;
        Ok(read)
    })
    .await?;

    // Wake the scheduler so worker rotations (stop old / start new cron/stage
    // workers) are picked up immediately.
    scheduler_notify.notify_scheduler();

    Ok(read)
}

pub async fn assert_function_name_not_exists<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    collection_id: &CollectionId,
    function: &FunctionDBWithNames,
    function_update: &FunctionUpdate,
) -> Result<(), TdError> {
    if let Some(name) = &function_update.name
        && *name != function.base.base.name
    {
        conn.scd2()
            .assert_not_exists::<FunctionDBWithNames>(and!(collection_id, name))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::function::register::RegisterFunctionError;
    use crate::function::services::tests::{assert_register, assert_update};
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::dependency::DependencyDB;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister};
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::trigger::TriggerDB;
    use td_objects::dxo::worker::WorkerDB;
    use td_objects::system::TD_FN_STATE_PREFIX;
    use td_objects::types::basic::{
        AccessTokenId, BasePath, Decorator, Depth, FunctionName, FunctionRuntimeValues,
        FunctionStatus, ResetStage, SNIPPET_MAX_LEN, TableId, TableName, TableStatus, UserId,
        WorkerMessageStatus, WorkerStatus, WorkerType,
    };
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, ResetState, ReuseFrozen, TableNameDto, WorkerConfig,
        WorkerTypeDto,
    };
    use td_objects::types::basic::{CollectionIdName, FunctionIdName};
    use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
    use td_objects::types::recursion::RecursionOptions;
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_update_fields(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "foo".try_into()?,
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("foo_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("foo_updated snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("foo")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_snippet_larger_than_storage_is_trimmed(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "foo".try_into()?,
            description: "foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("foo runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        db.scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let mut module = String::from("import tabsdata as td\n\n\n@td.transformer()\ndef f(df):\n");
        for line in 0..2000 {
            module.push_str(&format!("    value_{line} = df.select('col_{line}')\n"));
        }
        module.push_str("    return value_0\n");
        assert!(module.len() > SNIPPET_MAX_LEN);

        let update = FunctionUpdate {
            bundle_id: Some(BundleId::default()),
            snippet: Some(module.as_str().try_into()?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("foo")?,
                    },
                ),
                update,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await?.data;

        let snippet = &*response.snippet;
        assert!(snippet.len() <= SNIPPET_MAX_LEN);
        assert!(snippet.starts_with("import tabsdata as td\n"));
        assert!(snippet.ends_with("    return value_0\n"));
        assert!(snippet.contains(" bytes elided ..."));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_add_new_table(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
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

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_remove_table(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_maintain_table(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    async fn active_table_id(
        db: &TdPool,
        pred: impl Fn(&TableName) -> bool,
    ) -> Result<TableId, TdError> {
        let active: Vec<TableDB> = db
            .timeline()
            .fetch_all::<TableDB>(&TableStatus::Active)
            .await?;
        let table = active
            .into_iter()
            .find(|t| pred(&t.base.name))
            .expect("matching active table exists");
        Ok(table.base.table_id)
    }

    #[td_test::test(db)]
    async fn test_update_reset_state(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("reset_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "reset_fn".try_into()?,
            description: "desc".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("user_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        // Register via the service so the system state table is created.
        let register = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(register)
            .await?;

        let user_table = TableName::try_from("user_table")?;
        let state_id_before = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;
        let user_id_before = active_table_id(&db, |n| *n == user_table).await?;

        let update = FunctionUpdate {
            name: Some("reset_fn".try_into()?),
            tables: Some(vec![TableNameDto::try_from("user_table")?]),
            reset_state: ResetState::TRUE,
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("reset_fn")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let _ = service.update_function(request).await?.data;

        let state_id_after = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;
        let user_id_after = active_table_id(&db, |n| *n == user_table).await?;

        assert_ne!(
            state_id_before, state_id_after,
            "reset_state should regenerate the state table id"
        );
        assert_eq!(
            user_id_before, user_id_after,
            "user table id should be preserved"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_no_reset_state_reuses_state_id(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("noreset_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "noreset_fn".try_into()?,
            description: "desc".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("user_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let register = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(register)
            .await?;

        let state_id_before = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;

        let update = FunctionUpdate {
            name: Some("noreset_fn".try_into()?),
            tables: Some(vec![TableNameDto::try_from("user_table")?]),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("noreset_fn")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let _ = service.update_function(request).await?.data;

        let state_id_after = active_table_id(&db, |n| n.starts_with(TD_FN_STATE_PREFIX)).await?;
        assert_eq!(
            state_id_before, state_id_after,
            "without reset_state the state table id is reused"
        );
        Ok(())
    }

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
    async fn test_update_reset_stage_keeps_stage_base_path(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("stage_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "stage_fn".try_into()?,
            description: "desc".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let register = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(register)
            .await?;

        let fn_name = FunctionName::try_from("stage_fn")?;
        let before = active_function(&db, &fn_name).await?;
        let stage_id_before = before.base.stage_id;
        let stage_base_path_before = before.base.stage_base_path.clone();

        let update = FunctionUpdate {
            name: Some("stage_fn".try_into()?),
            reset_stage: ResetStage::TRUE,
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("stage_fn")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let _ = service.update_function(request).await?.data;

        let after = active_function(&db, &fn_name).await?;
        assert_ne!(
            stage_id_before, after.base.stage_id,
            "reset_stage should regenerate the stage id"
        );
        assert_eq!(
            stage_base_path_before, after.base.stage_base_path,
            "reset_stage should keep the stage base_path"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_no_reset_stage_carries_stage_id(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("nostage_coll")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "nostage_fn".try_into()?,
            description: "desc".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let register = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(register)
            .await?;

        let fn_name = FunctionName::try_from("nostage_fn")?;
        let stage_id_before = active_function(&db, &fn_name).await?.base.stage_id;

        let update = FunctionUpdate {
            name: Some("nostage_fn".try_into()?),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("nostage_fn")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let _ = service.update_function(request).await?.data;

        let stage_id_after = active_function(&db, &fn_name).await?.base.stage_id;
        assert_eq!(
            stage_id_before, stage_id_after,
            "without reset_stage the stage id is reused"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_base_path_override_sets_function_base_path(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("override_coll")?,
                &UserId::admin(),
            )
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "override_fn".try_into()?,
            description: "desc".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let register = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(register)
            .await?;

        let fn_name = FunctionName::try_from("override_fn")?;

        let update = FunctionUpdate {
            name: Some("override_fn".try_into()?),
            base_path: Some(BasePath::try_from("/over")?),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("override_fn")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let _ = service.update_function(request).await?.data;

        let after = active_function(&db, &fn_name).await?;
        assert_eq!(
            after.base.base_path,
            Some(BasePath::try_from("/over")?),
            "base_path override should be persisted on the new version"
        );

        let update = FunctionUpdate {
            name: Some("override_fn".try_into()?),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("override_fn")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let _ = service.update_function(request).await?.data;

        let after = active_function(&db, &fn_name).await?;
        assert_eq!(
            after.base.base_path,
            Some(BasePath::try_from("/over")?),
            "without an override the previous base_path carries forward"
        );
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_add_dependencies(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            dependencies: Some(vec![TableDependencyDto::try_from("new_table")?]),
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_remove_dependencies(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("new_table")?]),
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_maintain_dependencies(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("new_table")?]),
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("foo_updated description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            dependencies: Some(vec![TableDependencyDto::try_from("new_table")?]),
            tables: Some(vec![TableNameDto::try_from("new_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_add_trigger(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
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

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("function_foo description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_remove_trigger(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("function_foo description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_maintain_trigger(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("trigger_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("function_foo description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_change_everything(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let trigger_create = FunctionRegister {
            child_functions: None,
            name: "the_trigger".try_into()?,
            description: "wanted".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "the_trigger snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![
                TableNameDto::try_from("trigger_table")?,
                TableNameDto::try_from("trigger_table_2")?,
            ]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &trigger_create)
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![TableDependencyDto::try_from("trigger_table")?]),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table")?]),
            tables: Some(vec![
                TableNameDto::try_from("joaquin_table")?,
                TableNameDto::try_from("joaquin_table_2")?,
            ]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout_updated".try_into()?),
            description: Some("function_foo description updated".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet updated".try_into()?),
            decorator: Some(Decorator::Transformer),
            dependencies: Some(vec![
                TableDependencyDto::try_from("trigger_table")?,
                TableDependencyDto::try_from("trigger_table_2")?,
            ]),
            triggers: Some(vec![TableTriggerDto::try_from("trigger_table_2")?]),
            tables: Some(vec![
                TableNameDto::try_from("joaquin_table")?,
                TableNameDto::try_from("joaquin_table_3")?,
            ]),
            runtime_values: Some(FunctionRuntimeValues::try_from("new mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_freeze_unfreeze(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "joaquin_workout".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("joaquin_table")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("joaquin_workout".try_into()?),
            description: Some("function_foo description".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("function_foo snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let _response = response?;

        let tables = db
            .timeline()
            .fetch_all::<TableDB>(and!(
                TableName::try_from("joaquin_table")?,
                TableStatus::Frozen
            ))
            .await?;
        assert_eq!(tables.len(), 1);

        let update = FunctionUpdate {
            name: Some("joaquin_workout".try_into()?),
            tables: Some(vec![TableNameDto::try_from("joaquin_table")?]),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response;
        assert!(response.is_err());
        let error = response.unwrap_err();
        let error = error.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            error,
            RegisterFunctionError::FrozenTableAlreadyExists(..)
        ));

        let update = FunctionUpdate {
            name: Some("joaquin_workout".try_into()?),
            tables: Some(vec![TableNameDto::try_from("joaquin_table")?]),
            reuse_frozen_tables: ReuseFrozen::TRUE,
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin_workout")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let _response = response?;

        let tables: Vec<TableDB> = db
            .timeline()
            .fetch_all::<TableDB>(&(&TableName::try_from("joaquin_table")?))
            .await?;
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].base.status, TableStatus::Active);
        assert_eq!(tables[1].base.status, TableStatus::Frozen);
        assert_eq!(tables[2].base.status, TableStatus::Active);
        assert_eq!(tables[0].base.table_id, tables[1].base.table_id);
        assert_eq!(tables[1].base.table_id, tables[2].base.table_id);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_same_name(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            child_functions: None,
            name: "function_foo".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id,
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: dependencies.clone(),
            triggers: triggers.clone(),
            tables: tables.clone(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let update = FunctionUpdate {
            name: Some("function_foo_2".try_into()?),
            tables: Some(vec![TableNameDto::try_from("joaquin_table")?]),
            ..Default::default()
        };

        let update_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_foo")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(update_request).await;
        let update_response = response?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str(collection_name.as_str())?,
                },
                create.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let response = service.register_function(request).await;
        let create_response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &update_response,
        )
        .await?;
        assert_register(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &create_response,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_update_private_tables_all_same_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, false, false).await
    }

    #[td_test::test(db)]
    async fn test_update_private_tables_deps_diff_collection(db: TdPool) -> Result<(), TdError> {
        test_private_tables(db, true, false).await
    }

    #[td_test::test(db)]
    async fn test_update_private_tables_triggers_diff_collection(
        db: TdPool,
    ) -> Result<(), TdError> {
        test_private_tables(db, false, true).await
    }

    async fn test_private_tables(
        db: TdPool,
        deps_diff_collection: bool,
        triggers_diff_collection: bool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;
        let collection_name_2 = CollectionName::try_from("collection_2")?;
        let collection_2 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_2, &UserId::admin())
            .await?;

        for collection in [&collection_1, &collection_2] {
            let dependencies = None;
            let triggers = None;
            let tables = Some(vec![
                TableNameDto::try_from("_table_1")?,
                TableNameDto::try_from("_table_2")?,
            ]);

            let bundle_id = BundleId::default();
            let create = FunctionRegister {
                child_functions: None,
                name: "function_1".try_into()?,
                description: "function_1 description".try_into()?,
                bundle_id,
                snippet: "function_1 snippet".try_into()?,
                decorator: Decorator::Transformer,
                connector: None,
                connection: None,
                base_path: None,
                dependencies: dependencies.clone(),
                triggers: triggers.clone(),
                tables: tables.clone(),
                table_types: None,
                runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
                reuse_frozen_tables: ReuseFrozen::FALSE,
                enabled: None,
                workers: None,
            };
            db.scoped(p.id).seed().function(collection, &create).await?;
        }

        let dependencies = Some(vec![
            TableDependencyDto::try_from("_table_1")?,
            TableDependencyDto::try_from("_table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from("_table_1")?,
            TableTriggerDto::try_from("_table_2")?,
        ]);
        let tables = Some(vec![TableNameDto::try_from("output_1")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            child_functions: None,
            name: "function_2".try_into()?,
            description: "function_2 description".try_into()?,
            bundle_id,
            snippet: "function_2 snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        db.scoped(p.id)
            .seed()
            .function(&collection_2, &create)
            .await?;

        let deps_collection = if deps_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };
        let triggers_collection = if triggers_diff_collection {
            "collection_1"
        } else {
            "collection_2"
        };

        let dependencies = Some(vec![
            TableDependencyDto::try_from(format!("{deps_collection}/_table_1"))?,
            TableDependencyDto::try_from("_table_2")?,
        ]);
        let triggers = Some(vec![
            TableTriggerDto::try_from(format!("{triggers_collection}/_table_1"))?,
            TableTriggerDto::try_from("_table_2")?,
        ]);
        let tables = Some(vec![TableNameDto::try_from("output_1")?]);

        let bundle_id = BundleId::default();
        let update = FunctionUpdate {
            name: Some("function_2".try_into()?),
            description: Some("function_2 description".try_into()?),
            bundle_id: Some(bundle_id),
            snippet: Some("function_2 snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            dependencies,
            triggers,
            tables,
            runtime_values: Some(FunctionRuntimeValues::try_from("mock runtime values")?),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection_2.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_2")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.update_function(request).await;
        if deps_diff_collection || triggers_diff_collection {
            assert!(res.is_err());
            assert!(res.err().unwrap().code().contains("PrivateTableError"))
        } else {
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_private_table_flag(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name_1 = CollectionName::try_from("collection_1")?;
        let collection_1 = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name_1, &UserId::admin())
            .await?;

        let tables = Some(vec![TableNameDto::try_from("table0")?]);

        let bundle_id = BundleId::default();
        let create = FunctionRegister {
            child_functions: None,
            name: "function".try_into()?,
            description: "description".try_into()?,
            bundle_id,
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: Some(vec![]),
            triggers: Some(vec![]),
            tables,
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("mock runtime values")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        db.scoped(p.id)
            .seed()
            .function(&collection_1, &create)
            .await?;

        let tables = Some(vec![
            TableNameDto::try_from("table0")?,
            TableNameDto::try_from("_table0")?,
        ]);

        let bundle_id = BundleId::default();
        let update = FunctionUpdate {
            bundle_id: Some(bundle_id),
            tables,
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection_1.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.update_function(request).await;
        assert!(res.is_ok());

        let tables: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(&(res.unwrap().function_id))
            .await?;
        let map: HashMap<String, TableDB> = tables
            .into_iter()
            .map(|table| (table.base.name.to_string(), table))
            .collect();
        assert!(!map.get("table0").unwrap().base.private.deref());
        assert!(map.get("_table0").unwrap().base.private.deref());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_maintain_triggers_dependencies(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let created_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let dependant = FunctionRegister {
            child_functions: None,
            name: "function_2".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_1")?].into(),
            triggers: vec![TableTriggerDto::try_from("table_1")?].into(),
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &dependant)
            .await?;

        let update = FunctionUpdate {
            name: Some("function_1".try_into()?),
            description: Some("description".try_into()?),
            bundle_id: Some(BundleId::default()),
            snippet: Some("snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: Some(vec![TableNameDto::try_from("table_1")?]),
            ..Default::default()
        };

        // Triggers and dependencies survive the update: they reference ids,
        // not version ids.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("~{}", collection.id))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &create,
            &created_function,
            &update,
            &response,
        )
        .await?;

        let recursive_downstream_trigger: Vec<TriggerDB> = db
            .scd2()
            .as_of(&AtTime::now())
            .recursive::<TriggerDB, FunctionDB>(RecursionOptions::new(Some(Depth::ZERO), None))
            .start_at(&response.function_id)
            .where_edges(TriggerDB::active())
            .where_nodes(FunctionDB::executable())
            .fetch_all()
            .await
            .unwrap();
        assert_eq!(recursive_downstream_trigger.len(), 1);
        let triggered_function_id = &recursive_downstream_trigger[0].base.function_id;
        let triggered_function: FunctionDBWithNames = db
            .scd2()
            .fetch_one::<FunctionDBWithNames>(&(triggered_function_id))
            .await
            .unwrap();
        assert_eq!(triggered_function.base.base.name, dependant.name);

        let downstream_dependencies: Vec<DependencyDB> =
            db.scd2().fetch_all(&triggered_function_id).await.unwrap();
        assert_eq!(downstream_dependencies.len(), 1);
        let dependency_function_id = &recursive_downstream_trigger[0].base.function_id;
        let dependency_function: FunctionDBWithNames = db
            .scd2()
            .fetch_one::<FunctionDBWithNames>(&dependency_function_id)
            .await
            .unwrap();
        assert_eq!(dependency_function.base.base.name, dependant.name);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_update_remove_triggers_dependencies(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let _ = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let dependant = FunctionRegister {
            child_functions: None,
            name: "function_2".try_into()?,
            description: "description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: vec![TableDependencyDto::try_from("table_1")?].into(),
            triggers: vec![TableTriggerDto::try_from("table_1")?].into(),
            tables: None,
            table_types: None,
            runtime_values: FunctionRuntimeValues::default(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let dependant_function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &dependant)
            .await?;

        let update = FunctionUpdate {
            name: Some("function_2".try_into()?),
            ..Default::default()
        };

        // Triggers and dependencies survive the update: they reference ids,
        // not version ids.
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_2")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        assert_update(
            &db,
            &UserId::admin(),
            &collection,
            &dependant,
            &dependant_function,
            &update,
            &response,
        )
        .await?;

        let recursive_downstream_trigger: Vec<TriggerDB> = db
            .scd2()
            .as_of(&AtTime::now())
            .recursive::<TriggerDB, FunctionDB>(RecursionOptions::new(Some(Depth::ZERO), None))
            .start_at(&response.function_id)
            .where_edges(TriggerDB::active())
            .where_nodes(FunctionDB::executable())
            .fetch_all()
            .await
            .unwrap();
        assert!(recursive_downstream_trigger.is_empty());

        let dependency_function: Vec<FunctionDBWithNames> = db
            .scd2()
            .fetch_all::<FunctionDBWithNames>(&(&response.function_id))
            .await
            .unwrap();
        assert_eq!(dependency_function.len(), 1); // only implicit self dependency
        assert_eq!(dependency_function[0].base.base.name, dependant.name);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_workers_created(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
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
        let function_db = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&function_db.base.id)
            .await
            .unwrap();
        assert!(workers.is_empty());

        let update = FunctionUpdate {
            workers: Some(HashMap::from([(
                WorkerTypeDto::StageTrigger,
                WorkerConfig::default(),
            )])),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let updated_function_1 = service.update_function(request).await?.data;
        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_1.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, updated_function_1.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::Scheduled);
        assert_eq!(worker.base.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));

        let update = FunctionUpdate {
            workers: None,
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let updated_function_2 = service.update_function(request).await?.data;

        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_1.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, updated_function_1.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.base.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));

        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_2.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, updated_function_2.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::Scheduled);
        assert_eq!(worker.base.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));

        let update = FunctionUpdate {
            workers: Some(HashMap::from([(
                WorkerTypeDto::CronTrigger,
                WorkerConfig::default(),
            )])),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let updated_function_3 = service.update_function(request).await?.data;

        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_2.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, updated_function_2.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.base.worker_type, WorkerType::StageTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));

        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_3.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, updated_function_3.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::Scheduled);
        assert_eq!(worker.base.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));

        let update = FunctionUpdate {
            workers: Some(HashMap::new()),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let updated_function_4 = service.update_function(request).await?.data;

        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_3.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.base.collection_id, collection.id);
        assert_eq!(worker.base.execution_id, None);
        assert_eq!(worker.base.transaction_id, None);
        assert_eq!(worker.base.physical_function_run_id, None);
        assert_eq!(worker.base.function_version_id, updated_function_3.id);
        assert_eq!(worker.base.message_status, WorkerMessageStatus::None);
        assert_eq!(worker.base.started_on, None);
        assert_eq!(worker.base.ended_on, None);
        assert_eq!(worker.base.status, WorkerStatus::StopScheduled);
        assert_eq!(worker.base.worker_type, WorkerType::CronTrigger);
        assert_eq!(worker.base.worker_config, Some(WorkerConfig::default()));

        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&updated_function_4.id)
            .await
            .unwrap();
        assert_eq!(workers.len(), 0);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_multiple_workers_error(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection_name = CollectionName::try_from("cofnig")?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&collection_name, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            child_functions: None,
            name: "function_1".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
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
        let function_db = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let workers: Vec<WorkerDB> = db
            .fetch_all::<WorkerDB>(&function_db.base.id)
            .await
            .unwrap();
        assert!(workers.is_empty());

        let update = FunctionUpdate {
            workers: Some(HashMap::from([
                (WorkerTypeDto::CronTrigger, WorkerConfig::default()),
                (WorkerTypeDto::StageTrigger, WorkerConfig::default()),
            ])),
            ..Default::default()
        };
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&collection.name.to_string())?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("function_1")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let res = service.update_function(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(format!("{err}").contains("Multiple workers are not supported."));
        Ok(())
    }

    // An update that changes a publisher's connector to one that no longer
    // matches the collection's connection kind is rejected.
    #[td_test::test(db)]
    async fn test_update_publisher_connector_mismatch_err(db: TdPool) -> Result<(), TdError> {
        use crate::collection::services::connection::test_support::create_collection;
        use std::sync::Arc;
        use td_bridge::test::TestBridgeProvider;
        use td_bridge::{BridgeProviderHandle, Orchestrator};
        use td_objects::types::basic::{ConnectionKind, GroupName};

        let p = db.seed().test_project().await?;
        create_collection(&db, p.id, "src_upd", Some(GroupName::source())).await;

        let create = FunctionRegister {
            child_functions: None,
            name: "upd_pub".try_into()?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: Some(ConnectionKind::try_from("mysqlSrcConn")?),
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
        let register_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .create(
                CollectionParam {
                    collection: CollectionIdName::from_str("src_upd")?,
                },
                create,
            )
            .in_project(p.id);
        Services::with_defaults(db.clone())
            .register_function(register_request)
            .await?;

        let update = FunctionUpdate {
            bundle_id: Some(BundleId::default()),
            snippet: Some("s2".try_into()?),
            connection: Some(Some(ConnectionKind::try_from("postgresSrcConn")?)),
            ..Default::default()
        };
        let update_request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str("src_upd")?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("upd_pub")?,
                    },
                ),
                update,
            )
            .in_project(p.id);
        // Run the update under Kubernetes, where the connection-kind
        // contract is enforced (see `ensure_connection_kind_matches`).
        let mut context = Services::with_defaults(db.clone());
        context.bridge = Ctx::new(BridgeProviderHandle::new(Arc::new(
            TestBridgeProvider::default().with_orchestrator(Orchestrator::Kubernetes),
        )));
        let err = context
            .clone()
            .update_function(update_request)
            .await
            .unwrap_err();
        let err = err.domain_err::<RegisterFunctionError>();
        assert!(matches!(
            err,
            RegisterFunctionError::ConnectionKindMismatch(..)
        ));
        Ok(())
    }

    /// What an update keeps and what it moves. Equal across a code-only
    /// update: the function id, every table's `table_id`, its version row and
    /// the `function_version_id` on it (the version that defined it). Moved:
    /// the function version. Only a table whose definition changed gets a new
    /// row, stamped with the version that changed it.
    #[td_test::test(db)]
    async fn test_update_table_versions_follow_definitions_not_code(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("defs_coll")?, &UserId::admin())
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "defs_fn".try_into()?,
            description: "desc".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: Some(vec![TableNameDto::try_from("t_a")?]),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let collection_param = || CollectionParam {
            collection: CollectionIdName::from_str(&format!("~{}", collection.id)).unwrap(),
        };
        let service = Services::with_defaults(db.clone());
        service
            .register_function(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(collection_param(), create)
                    .in_project(p.id),
            )
            .await?;
        let current_function = async |db: &TdPool| -> Result<FunctionDB, TdError> {
            let functions: Vec<FunctionDB> =
                db.scd2().fetch_all::<FunctionDB>(&collection.id).await?;
            Ok(functions.into_iter().next().expect("one function"))
        };
        let current_tables = async |db: &TdPool, function_id| -> Result<Vec<TableDB>, TdError> {
            let mut tables: Vec<TableDB> = db.scd2().fetch_all::<TableDB>(&function_id).await?;
            tables.sort_by(|a, b| a.base.name.cmp(&b.base.name));
            Ok(tables)
        };
        let update_with = |update: FunctionUpdate| {
            RequestContext::with(AccessTokenId::default(), UserId::admin())
                .update(
                    (
                        collection_param(),
                        FunctionParam {
                            function: FunctionIdName::from_str("defs_fn").unwrap(),
                        },
                    ),
                    update,
                )
                .in_project(p.id)
        };

        let v1 = current_function(&db).await?;
        let tables_v1 = current_tables(&db, v1.base.function_id).await?;
        assert_eq!(tables_v1.len(), 2, "t_a and the state table");
        for t in &tables_v1 {
            assert_eq!(t.base.function_id, v1.base.function_id);
            assert_eq!(t.base.function_version_id, v1.base.id);
        }

        service
            .update_function(update_with(FunctionUpdate {
                snippet: Some("snippet 2".try_into()?),
                tables: Some(vec![TableNameDto::try_from("t_a")?]),
                ..Default::default()
            }))
            .await?;
        let v2 = current_function(&db).await?;
        assert_ne!(v2.base.id, v1.base.id);
        assert_eq!(v2.base.function_id, v1.base.function_id);
        let tables_v2 = current_tables(&db, v2.base.function_id).await?;
        assert_eq!(tables_v2.len(), tables_v1.len());
        for (before, after) in tables_v1.iter().zip(&tables_v2) {
            assert_eq!(after.base.id, before.base.id, "same version row");
            assert_eq!(after.base.table_id, before.base.table_id);
            assert_eq!(
                after.base.function_id, v2.base.function_id,
                "always the producer"
            );
            assert_eq!(
                after.base.function_version_id, v1.base.id,
                "the defining version, which is no longer the current one"
            );
            assert_ne!(after.base.function_version_id, v2.base.id);
        }

        service
            .update_function(update_with(FunctionUpdate {
                tables: Some(vec![
                    TableNameDto::try_from("t_a")?,
                    TableNameDto::try_from("t_b")?,
                ]),
                ..Default::default()
            }))
            .await?;
        let v3 = current_function(&db).await?;
        let tables_v3 = current_tables(&db, v3.base.function_id).await?;
        assert_eq!(tables_v3.len(), 3);
        for t in &tables_v3 {
            assert_eq!(t.base.function_id, v3.base.function_id);
            let expected_defining = if *t.base.name == "t_b" {
                v3.base.id.clone()
            } else {
                v1.base.id.clone()
            };
            assert_eq!(
                t.base.function_version_id, expected_defining,
                "{}",
                t.base.name
            );
        }
        Ok(())
    }
}
