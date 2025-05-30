//
// Copyright 2025 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::rest_urls::TransactionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, CollReadAll,
};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{CollectionId, TransactionIdName};
use td_objects::types::execution::{TransactionDB, WorkerMessage};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = WorkerMessageListService,
    request = ListRequest<TransactionParam>,
    response = ListResponse<WorkerMessage>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        // Extract parameters
        from_fn(With::<ListRequest<TransactionParam>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<TransactionParam>>::extract_name::<TransactionParam>),
        // find collection ID
        from_fn(With::<TransactionParam>::extract::<TransactionIdName>),
        from_fn(By::<TransactionIdName>::select::<DaoQueries, TransactionDB>),
        from_fn(With::<TransactionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, CollReadAll>::check),
        // List all WorkerMessage in a transaction.
        from_fn(By::<TransactionIdName>::list::<TransactionParam, DaoQueries, WorkerMessage>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_worker_message::seed_worker_message;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, RoleId, TransactionKey, UserId,
    };
    use td_objects::types::execution::{FunctionRunStatus, WorkerMessageStatus};
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_worker_messages(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let provider = WorkerMessageListService::provider(db, queries, authz_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<TransactionParam>, ListResponse<WorkerMessage>>(&[
            // Extract parameters
            type_of_val(&With::<ListRequest<TransactionParam>>::extract::<RequestContext>),
            type_of_val(&With::<ListRequest<TransactionParam>>::extract_name::<TransactionParam>),
            // find collection ID
            type_of_val(&With::<TransactionParam>::extract::<TransactionIdName>),
            type_of_val(&By::<TransactionIdName>::select::<DaoQueries, TransactionDB>),
            type_of_val(&With::<TransactionDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, CollReadAll>::check),
            // List all WorkerMessage in a transaction.
            type_of_val(
                &By::<TransactionIdName>::list::<TransactionParam, DaoQueries, WorkerMessage>,
            ),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_worker_messages(db: DbPool) -> Result<(), TdError> {
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let function_version = seed_function(&db, &collection, &create).await;
        let execution = seed_execution(&db, &function_version).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            &collection,
            &function_version,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let worker_messages = [
            seed_worker_message(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker_message(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker_message(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker_message(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker_message(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Unlocked,
            )
            .await,
        ];

        let service = WorkerMessageListService::new(
            db.clone(),
            Arc::new(DaoQueries::default()),
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .list(
            TransactionParam::builder()
                .try_transaction(format!("{}", transaction.id()))?
                .build()?,
            ListParams::default(),
        );

        let response = service.raw_oneshot(request).await?;
        assert_eq!(*response.len(), worker_messages.len());
        assert_eq!(response.data()[0].id(), worker_messages[0].id());
        assert_eq!(response.data()[0].status(), worker_messages[0].status());
        assert_eq!(response.data()[1].id(), worker_messages[1].id());
        assert_eq!(response.data()[1].status(), worker_messages[1].status());
        assert_eq!(response.data()[2].id(), worker_messages[2].id());
        assert_eq!(response.data()[2].status(), worker_messages[2].status());
        assert_eq!(response.data()[3].id(), worker_messages[3].id());
        assert_eq!(response.data()[3].status(), worker_messages[3].status());
        assert_eq!(response.data()[4].id(), worker_messages[4].id());
        assert_eq!(response.data()[4].status(), worker_messages[4].status());
        Ok(())
    }
}
