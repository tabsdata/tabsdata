//
// Copyright 2025 Tabsdata Inc.
//

use bytes::Bytes;
use futures_util::StreamExt;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{DevelopProject, Project};
use td_bridge::BridgeProviderHandle;
use td_bridge::worker::{WorkerLogStream, WorkerLogTarget};
use td_common::worker::WorkerName;
use td_database::access::{AcrossProjects, DbPool};
use td_error::TdError;
use td_objects::dxo::crudl::ReadRequest;
use td_objects::dxo::worker::LogsExtension;
use td_objects::dxo::worker::{LogsExtensionParam, LogsRetryParam, WorkerDB, WorkerParam};
use td_objects::types::basic::{LogsCastNumber, WorkerType};
use td_objects::types::stream::BoxedSyncStream;
use td_queries::Queries;
use tokio::sync::mpsc;

#[service_factory]
async fn worker_log(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<(WorkerParam, LogsExtensionParam, LogsRetryParam)>,
) -> Result<BoxedSyncStream, TdError> {
    let (ctx, (worker_param, extension, retry)) = request.into_parts();
    let mut trx = db.read().await?;
    let worker: WorkerDB = trx
        .get(authz.none())
        .fetch_one(&worker_param.worker)
        .await?;
    let mut trx = trx.enter_project(&worker.project_id).await?;
    let _grant = authz
        .require::<Project, DevelopProject>(&mut trx, &ctx, &worker.project_id)
        .await?;

    worker_logs(&bridge, &worker, &extension.extension, &retry.retry).await
}

#[service_factory]
async fn worker_log_tail(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<AcrossProjects>,
    request: ReadRequest<(WorkerParam, LogsExtensionParam, LogsRetryParam)>,
) -> Result<BoxedSyncStream, TdError> {
    let (ctx, (worker_param, extension, retry)) = request.into_parts();
    let mut trx = db.read().await?;
    let worker: WorkerDB = trx
        .get(authz.none())
        .fetch_one(&worker_param.worker)
        .await?;
    let mut trx = trx.enter_project(&worker.project_id).await?;
    let _grant = authz
        .require::<Project, DevelopProject>(&mut trx, &ctx, &worker.project_id)
        .await?;

    worker_logs_tail(&bridge, &worker, &extension.extension, &retry.retry).await
}

fn worker_log_target(
    worker: &WorkerDB,
    extensions: &[LogsExtension],
    casts: &[LogsCastNumber],
) -> WorkerLogTarget {
    let worker_kind = match worker.base.worker_type {
        WorkerType::FunctionRunner => WorkerName::Function,
        WorkerType::CronTrigger => WorkerName::CronTrigger,
        WorkerType::StageTrigger => WorkerName::StageTrigger,
    };
    WorkerLogTarget {
        worker_id: worker.base.id.to_string(),
        worker_kind,
        log_patterns: extensions.iter().map(LogsExtension::glob_pattern).collect(),
        casts: casts.iter().map(|cast| cast.to_string()).collect(),
    }
}

const LOG_STREAM_CHANNEL_CAPACITY: usize = 16;

fn into_boxed_sync_stream(stream: WorkerLogStream) -> BoxedSyncStream {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, TdError>>(LOG_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            if sender.send(item.map_err(TdError::from)).await.is_err() {
                break;
            }
        }
    });
    let mut receiver = receiver;
    let receiver_stream = futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).fuse();
    BoxedSyncStream::new(receiver_stream)
}

pub async fn worker_logs(
    bridge: &BridgeProviderHandle,
    worker: &WorkerDB,
    extensions: &[LogsExtension],
    casts: &[LogsCastNumber],
) -> Result<BoxedSyncStream, TdError> {
    let target = worker_log_target(worker, extensions, casts);
    let stream = bridge.worker_handler().logs(&target).await?;
    Ok(into_boxed_sync_stream(stream))
}

pub async fn worker_logs_tail(
    bridge: &BridgeProviderHandle,
    worker: &WorkerDB,
    extensions: &[LogsExtension],
    casts: &[LogsCastNumber],
) -> Result<BoxedSyncStream, TdError> {
    let target = worker_log_target(worker, extensions, casts);
    let stream = bridge.worker_handler().logs_tail(&target).await?;
    Ok(into_boxed_sync_stream(stream))
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
                                worker: workers[0].base.id.clone(),
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
