//
// Copyright 2025 Tabs Data Inc.
//

use crate::worker::layers::logs::{get_worker_logs, resolve_worker_log_path};
use ta_services::factory::service_factory;
use td_authz::tower::{AuthzOn, CollAdmin, CollDev, CollExec, CollRead};
use td_authz::{Authz, AuthzContext};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::worker::WorkerDB;
use td_objects::rest_urls::{LogsExtension, WorkerLogsParams};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{CollectionId, LogsCastNumber, WorkerIdName};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::By;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = WorkerLogService,
    request = ReadRequest<WorkerLogsParams>,
    response = BoxedSyncStream,
    connection = ConnectionProvider,
    context = AuthzContext,
)]
fn service() {
    layers!(
        // Extract parameters
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>),
        // find collection ID
        from_fn(With::<WorkerLogsParams>::extract::<WorkerIdName>),
        from_fn(By::<WorkerIdName>::fetch_one::<WorkerDB>),
        from_fn(With::<WorkerDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
        // Resolve worker message path.
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
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_common::server::WORKSPACE_URI_ENV;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId,
        TransactionKey, UserId, WorkerMessageStatus,
    };
    use td_queries::test_utils::SeedQueries;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_workers_logs(db: TdPool) {
        use td_tower::metadata::type_of_val;

        WorkerLogService::with_defaults(db)
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
                type_of_val(&By::<WorkerIdName>::fetch_one::<WorkerDB>),
                type_of_val(&With::<WorkerDB>::extract::<CollectionId>),
                // check requester has collection permissions
                type_of_val(&AuthzOn::<CollectionId>::set),
                type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead>::check),
                // Resolve worker message path.
                type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
                type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
                type_of_val(&resolve_worker_log_path),
                // Get worker message logs.
                type_of_val(&get_worker_logs),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_workers_logs(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .seed_collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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
        let function_version = db.seed_function(&collection, &create).await?;
        let execution = db.seed_execution(&function_version).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db.seed_transaction(&execution, &transaction_key).await?;

        let function_run = db
            .seed_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &FunctionRunStatus::Scheduled,
            )
            .await?;

        let workers = [
            db.seed_function_runner_worker(
                &execution,
                &transaction,
                &function_run,
                &WorkerMessageStatus::Locked,
            )
            .await?,
            db.seed_function_runner_worker(
                &execution,
                &transaction,
                &function_run,
                &WorkerMessageStatus::Locked,
            )
            .await?,
        ];

        let test_dir = td_common::tdtestdir!();
        let response =
            temp_env::async_with_vars([(WORKSPACE_URI_ENV, Some(test_dir.to_str().unwrap()))], {
                async move {
                    // Create sample log files for the first worker, which will have the file names
                    // as the content.
                    create_log_files(&workers[0], 1, 1).await;

                    let service = WorkerLogService::with_defaults(db.clone()).service();

                    let request = RequestContext::with(
                        AccessTokenId::default(),
                        UserId::admin(),
                        RoleId::user(),
                    )
                    .read(
                        WorkerLogsParams::builder()
                            .worker(WorkerIdName::from_str(&workers[0].id.to_string())?)
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
