//
// Copyright 2025 Tabs Data Inc.
//

use crate::worker::layers::logs::{worker_logs, worker_logs_tail};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project, Rbac, Scope};
use td_bridge::BridgeProviderHandle;
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::worker::WorkerDB;
use td_objects::rest_urls::{LogsExtension, WorkerLogsParams};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{LogsCastNumber, ProjectId, WorkerIdName};
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
    scope = "Project",
    context = AuthzContext,
    context = BridgeProviderHandle,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>),
        // find worker (resolves the owning project for the scope)
        from_fn(With::<WorkerLogsParams>::extract::<WorkerIdName>),
        from_fn(By::<WorkerIdName>::fetch_one::<WorkerDB>),
        from_fn(With::<WorkerDB>::extract::<ProjectId>),
        // check requester has access to the project
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        // Resolve worker message path.
        from_fn(With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
        from_fn(With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
        from_fn(worker_logs),
    )
}

#[service_factory(
    name = WorkerLogTailService,
    request = ReadRequest<WorkerLogsParams>,
    response = BoxedSyncStream,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
    context = BridgeProviderHandle,
)]
fn logs_tail_service() {
    layers!(
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>),
        from_fn(With::<WorkerLogsParams>::extract::<WorkerIdName>),
        from_fn(By::<WorkerIdName>::fetch_one::<WorkerDB>),
        from_fn(With::<WorkerDB>::extract::<ProjectId>),
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, DevelopProject>::check),
        from_fn(With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
        from_fn(With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
        from_fn(worker_logs_tail),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::layers::tests::create_log_files;
    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use std::str::FromStr;
    use std::sync::Arc;
    use ta_services::service::TdService;
    use td_bridge::test::TestBridgeProvider;
    use td_bridge::worker::workspace::WorkspaceWorkerHandler;
    use td_common::env::WORKSPACE_URI_ENV;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus, ProjectId,
        TransactionKey, UserId, WorkerMessageStatus,
    };
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_worker_logs(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        WorkerLogService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<WorkerLogsParams>, ProjectId>, BoxedSyncStream>(
                &[
                    type_of_val(&With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
                    type_of_val(
                        &With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>,
                    ),
                    // find worker (resolves the owning project for the scope)
                    type_of_val(&With::<WorkerLogsParams>::extract::<WorkerIdName>),
                    type_of_val(&By::<WorkerIdName>::fetch_one::<WorkerDB>),
                    type_of_val(&With::<WorkerDB>::extract::<ProjectId>),
                    // check requester has access to the project
                    type_of_val(&Scope::<Project>::set),
                    type_of_val(&Rbac::<Project, DevelopProject>::check),
                    // Resolve worker message path.
                    type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
                    type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
                    type_of_val(&worker_logs),
                ],
            );
    }

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_worker_logs_tail(db: TdPool) {
        use td_database::pool::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        WorkerLogTailService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<ReadRequest<WorkerLogsParams>, ProjectId>, BoxedSyncStream>(
                &[
                    type_of_val(&With::<ReadRequest<WorkerLogsParams>>::extract::<RequestContext>),
                    type_of_val(
                        &With::<ReadRequest<WorkerLogsParams>>::extract_name::<WorkerLogsParams>,
                    ),
                    type_of_val(&With::<WorkerLogsParams>::extract::<WorkerIdName>),
                    type_of_val(&By::<WorkerIdName>::fetch_one::<WorkerDB>),
                    type_of_val(&With::<WorkerDB>::extract::<ProjectId>),
                    type_of_val(&Scope::<Project>::set),
                    type_of_val(&Rbac::<Project, DevelopProject>::check),
                    type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsExtension>>),
                    type_of_val(&With::<WorkerLogsParams>::extract::<Vec<LogsCastNumber>>),
                    type_of_val(&worker_logs_tail),
                ],
            );
    }

    #[td_test::test(db)]
    async fn test_worker_logs(db: TdPool) -> Result<(), TdError> {
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
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
        let function_version = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function_version)
            .await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &function_version,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        let workers = [
            db.scoped(ProjectId::sandbox())
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(ProjectId::sandbox())
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
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

                    let bridge = BridgeProviderHandle::new(Arc::new(
                        TestBridgeProvider::default().with_worker_handler(WorkspaceWorkerHandler),
                    ));
                    let service = WorkerLogService::new(
                        db.clone(),
                        Arc::new(AuthzContext::default()),
                        Arc::new(bridge),
                    )
                    .service();

                    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                        .read(
                            WorkerLogsParams::builder()
                                .worker(WorkerIdName::from_str(&workers[0].id.to_string())?)
                                .extension(vec![LogsExtension::All])
                                .retry(vec![])
                                .build()?,
                        )
                        .in_project(ProjectId::sandbox());

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
