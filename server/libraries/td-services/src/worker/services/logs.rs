//
// Copyright 2025 Tabsdata Inc.
//

use crate::worker::logs::{worker_logs, worker_logs_tail};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::DevelopProject;
use td_bridge::BridgeProviderHandle;
use td_database::access::{GatedConnection, RO};
use td_error::TdError;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::worker::{LogsExtensionParam, LogsRetryParam, WorkerDB, WorkerParam};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::LiveEntry;

#[service_factory]
async fn worker_log(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<(WorkerParam, LogsExtensionParam, LogsRetryParam)>,
) -> Result<BoxedSyncStream, TdError> {
    let (ctx, (worker_param, extension, retry)) = request.into_parts();
    let conn = gated_conn.get(authz.none());
    let worker: WorkerDB = conn.live().fetch_one(&worker_param.worker).await?;
    let _grant = authz
        .require_in_project::<DevelopProject>(conn, &ctx, &worker.project_id)
        .await?;

    worker_logs(&bridge, &worker, &extension.extension, &retry.retry).await
}

#[service_factory]
async fn worker_log_tail(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    gated_conn: &mut GatedConnection<RO>,
    request: ReadRequest<(WorkerParam, LogsExtensionParam, LogsRetryParam)>,
) -> Result<BoxedSyncStream, TdError> {
    let (ctx, (worker_param, extension, retry)) = request.into_parts();
    let conn = gated_conn.get(authz.none());
    let worker: WorkerDB = conn.live().fetch_one(&worker_param.worker).await?;
    let _grant = authz
        .require_in_project::<DevelopProject>(conn, &ctx, &worker.project_id)
        .await?;

    worker_logs_tail(&bridge, &worker, &extension.extension, &retry.retry).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::worker::tests::create_log_files;
    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use std::sync::Arc;
    use td_bridge::test::TestBridgeProvider;
    use td_bridge::worker::workspace::WorkspaceWorkerHandler;
    use td_common::env::WORKSPACE_URI_ENV;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::worker::{LogsExtension, LogsExtensionParam, LogsRetryParam, WorkerParam};
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, LogicalFunctionRunStatus,
        TransactionKey, UserId, WorkerMessageStatus,
    };
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_worker_logs(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let dependencies = None;
        let triggers = None;
        let tables = None;

        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies,
            triggers,
            tables,
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function_version = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function_version).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(p.id)
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
            db.scoped(p.id)
                .seed()
                .function_runner_worker(
                    &execution,
                    &transaction,
                    &physical_function_run,
                    &WorkerMessageStatus::Locked,
                )
                .await?,
            db.scoped(p.id)
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
                    let service = Services {
                        bridge: Ctx::new(bridge),
                        ..Services::with_defaults(db.clone())
                    };

                    let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
                        .read((
                            WorkerParam {
                                worker: workers[0].id.clone(),
                            },
                            LogsExtensionParam {
                                extension: vec![LogsExtension::All],
                            },
                            LogsRetryParam { retry: vec![] },
                        ));

                    let response = service.worker_log(request).await?.data;
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
        // the helper tests handle paths and content assertion.
        assert!(content.contains("err.log"));
        assert!(content.contains("fn.log"));
        assert!(content.contains("out.log"));
        assert!(content.contains("td.log"));
        Ok(())
    }
}
