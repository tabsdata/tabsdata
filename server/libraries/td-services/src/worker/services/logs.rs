//
// Copyright 2025 Tabs Data Inc.
//

use crate::worker::layers::logs::{get_worker_logs, resolve_worker_log_path};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::{LogsExtension, WorkerLogsParams};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::{CollectionId, LogsCastNumber, WorkerId, WorkerIdName};
use td_objects::types::execution::WorkerDB;
use td_objects::types::stream::BoxedSyncStream;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = WorkerLogService,
    request = ReadRequest<WorkerLogsParams>,
    response = BoxedSyncStream,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>),
        // find collection ID
        from_fn(With::<WorkerLogsParams>::extract::<WorkerIdName>),
        from_fn(By::<WorkerIdName>::select::<WorkerDB>),
        from_fn(With::<WorkerDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // Resolve worker message path.
        from_fn(With::<WorkerDB>::extract::<WorkerId>),
        from_fn(With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
        from_fn(With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
        from_fn(resolve_worker_log_path),
        // Get worker message logs.
        from_fn(get_worker_logs),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::layers::tests::create_log_files;
    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use td_common::server::WORKSPACE_URI_ENV;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::test_utils::seed_worker::seed_worker;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        TransactionKey, UserId,
    };
    use td_objects::types::execution::WorkerMessageStatus;
    use td_objects::types::function::FunctionRegister;
    use td_tower::ctx_service::RawOneshot;
    use testdir::testdir;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_workers_logs(db: DbPool) {
        use td_tower::metadata::type_of_val;

        WorkerLogService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<ReadRequest<WorkerLogsParams>, BoxedSyncStream>(&[
                // Extract parameters
                type_of_val(&With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
                type_of_val(
                    &With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>,
                ),
                // find collection ID
                type_of_val(&With::<WorkerLogsParams>::extract::<WorkerIdName>),
                type_of_val(&By::<WorkerIdName>::select::<WorkerDB>),
                type_of_val(&With::<WorkerDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                // Resolve worker message path.
                type_of_val(&With::<WorkerDB>::extract::<WorkerId>),
                type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
                type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
                type_of_val(&resolve_worker_log_path),
                // Get worker message logs.
                type_of_val(&get_worker_logs),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_workers_logs(db: DbPool) -> Result<(), TdError> {
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

        let workers = [
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
            seed_worker(
                &db,
                &execution,
                &transaction,
                &function_run,
                WorkerMessageStatus::Locked,
            )
            .await,
        ];

        let test_dir = testdir!();
        let response =
            temp_env::async_with_vars([(WORKSPACE_URI_ENV, Some(test_dir.to_str().unwrap()))], {
                async move {
                    // Create sample log files for the first worker, which will have the file names
                    // as the content.
                    create_log_files(workers[0].id(), 1, 1).await;

                    let service = WorkerLogService::with_defaults(db.clone())
                        .await
                        .service()
                        .await;

                    let request = RequestContext::with(
                        AccessTokenId::default(),
                        UserId::admin(),
                        RoleId::user(),
                    )
                    .read(
                        WorkerLogsParams::builder()
                            .try_worker(workers[0].id().to_string())?
                            .extension(vec![LogsExtension::All])
                            .retry(vec![])
                            .build()?,
                    );

                    let response = service.raw_oneshot(request).await?;
                    Ok::<_, TdError>(response)
                }
            })
            .await?;

        let content = response.into_inner().try_collect::<Vec<Bytes>>().await?;
        let content = content
            .iter()
            .flat_map(|b| b.iter())
            .cloned()
            .collect::<Vec<_>>();
        let content = String::from_utf8_lossy(&content);

        // Just make sure all log files are present and service works,
        // layers tests handle paths and content assertion.
        assert!(content.contains("err.log"));
        assert!(content.contains("fn.log"));
        assert!(content.contains("out.log"));
        assert!(content.contains("td.log"));
        Ok(())
    }
}
