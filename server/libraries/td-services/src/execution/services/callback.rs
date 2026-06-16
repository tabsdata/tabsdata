//
// Copyright 2025 Tabs Data Inc.
//

use crate::execution::layers::redirect_at_finish::redirect_pointers_at_finish;
use crate::execution::layers::update_status::{
    update_function_run_status, update_table_data_version_status, update_worker_status,
};
use crate::logical::layers::index_lfrs_by_exec_fn;
use crate::scheduler::layers::request::notify_scheduler;
use std::collections::HashSet;
use ta_services::factory::service_factory;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_execution::SchedulerNotify;
use td_objects::dxo::crudl::{RequestContext, UpdateRequest};
use td_objects::dxo::logical::{
    LogicalFunctionRunDB, UpdateLogicalFunctionRunDB, UpdateLogicalFunctionRunDBBuilder,
};
use td_objects::dxo::physical_function_run::{
    PhysicalFunctionRunDB, UpdatePhysicalFunctionRunDB, UpdatePhysicalFunctionRunDBBuilder,
};
use td_objects::dxo::worker::{
    CallbackRequest, UpdateWorkerDB, UpdateWorkerDBBuilder, UpdateWorkerExecution, WorkerDB,
};
use td_objects::rest_urls::internal::PhysicalFunctionRunWorkerIdParam;
use td_objects::tower_service::with::With;
use td_objects::types::basic::{
    AtTime, ExecutionId, PhysicalFunctionRunId, PhysicalFunctionRunStatus, WorkerId,
};
use td_queries::{By, ChunkedQueries};
use td_tower::default_services::TransactionProvider;
use td_tower::extractors::{MutRef, Ref};
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// Resolve each reporting PhysicalFunctionRunDB to its LFR — and drop
/// callbacks whose reporting FR is no longer in flight (its
/// `PFR.status` is already terminal). The remaining LFRs flow into
/// `update_function_run_status` / status-cascade.
///
/// "In flight" = `PFR.status` is `Dispatched` or `Running`. PFRs in
/// `Requested` shouldn't have callbacks (worker hasn't been dispatched
/// yet) but accepting them is harmless. Terminal statuses (Done /
/// Failed / Error) indicate a late or duplicate callback — drop.
async fn resolve_logical_function_runs(
    mut conn: MutRef<DynamicConnection<'static>>,
    physical_function_runs: Ref<Vec<PhysicalFunctionRunDB>>,
) -> Result<Vec<LogicalFunctionRunDB>, TdError> {
    if physical_function_runs.is_empty() {
        return Ok(Vec::new());
    }
    let exec_ids: Vec<ExecutionId> = physical_function_runs
        .iter()
        .map(|f| f.execution_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let all_lfrs: Vec<LogicalFunctionRunDB> = conn
        .fetch_all_chunked::<LogicalFunctionRunDB, ExecutionId>(&exec_ids)
        .await?;
    let lfr_by_exec_fn = index_lfrs_by_exec_fn(all_lfrs);

    Ok(physical_function_runs
        .iter()
        .filter_map(|fr| {
            if !PhysicalFunctionRunStatus::IN_FLIGHT.contains(&fr.status) {
                return None;
            }
            let lfr = lfr_by_exec_fn
                .get(&fr.execution_id)
                .and_then(|inner| inner.get(&fr.function_id))?;
            Some(lfr.clone())
        })
        .collect())
}

/// Per-attempt status + timing write on `physical_function_runs`. Always runs
/// (no sticky semantics on FR); the LFR's sticky-Committed gate is
/// enforced separately in `update_function_run_status`.
async fn update_function_run_audit(
    mut conn: MutRef<DynamicConnection<'static>>,
    physical_function_runs: Ref<Vec<PhysicalFunctionRunDB>>,
    update: Ref<UpdatePhysicalFunctionRunDB>,
) -> Result<(), TdError> {
    let fr_ids: Vec<PhysicalFunctionRunId> = physical_function_runs
        .iter()
        .map(|fr| fr.id.clone())
        .collect();
    conn.update_chunked::<_, PhysicalFunctionRunDB, _>(&*update, &fr_ids)
        .await?;
    Ok(())
}

#[service_factory(
    name = ExecutionCallbackService,
    request = UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>,
    response = (),
    connection = TransactionProvider,
    scope = "Project",
    context = SchedulerNotify,
)]
fn service() {
    layers!(
        // Extract from request.
        from_fn(
            With::<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>>::extract::<
                RequestContext,
            >
        ),
        from_fn(With::<RequestContext>::extract::<AtTime>),
        from_fn(
            With::<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>>::extract_name::<
                PhysicalFunctionRunWorkerIdParam,
            >
        ),
        from_fn(
            With::<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>>::extract_data::<
                CallbackRequest,
            >
        ),
        // Convert callback request to status update request.
        from_fn(With::<CallbackRequest>::try_convert_to::<UpdateWorkerExecution, _>),
        // Extract worker_id.
        from_fn(With::<PhysicalFunctionRunWorkerIdParam>::extract::<WorkerId>),
        // Find worker (we will always have 1).
        from_fn(By::<WorkerId>::assert_exists::<WorkerDB>),
        // Update worker status.
        from_fn(With::<UpdateWorkerExecution>::try_convert_to::<UpdateWorkerDBBuilder, _>),
        from_fn(With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB>),
        from_fn(update_worker_status),
        // Per-attempt audit on the PhysicalFunctionRunDB row: started_on,
        // ended_on, and status (the per-attempt status — never sticky).
        from_fn(With::<PhysicalFunctionRunWorkerIdParam>::extract::<PhysicalFunctionRunId>),
        from_fn(By::<PhysicalFunctionRunId>::fetch_all::<PhysicalFunctionRunDB>),
        from_fn(
            With::<UpdateWorkerExecution>::try_convert_to::<UpdatePhysicalFunctionRunDBBuilder, _>
        ),
        from_fn(With::<UpdatePhysicalFunctionRunDBBuilder>::build::<UpdatePhysicalFunctionRunDB>),
        from_fn(update_function_run_audit),
        // Canonical flow-control status update on the LFR (sticky-Committed
        // gate applies inside `update_function_run_status`).
        from_fn(resolve_logical_function_runs),
        from_fn(
            With::<UpdateWorkerExecution>::try_convert_to::<UpdateLogicalFunctionRunDBBuilder, _>
        ),
        from_fn(With::<UpdateLogicalFunctionRunDBBuilder>::build::<UpdateLogicalFunctionRunDB>),
        from_fn(update_function_run_status),
        // Redirect-at-finish: if the reporting FR is a replay attempt
        // (LFR pointer is currently bound to a different FR), append
        // the redirect events on success. No-op on failure.
        from_fn(redirect_pointers_at_finish),
        // Update table data versions status.
        from_fn(update_table_data_version_status),
        // Notify scheduler of status change.
        from_fn(notify_scheduler),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::layers::update_status::tests::{
        TestExecution, TestFunction, TestTransaction, test_status_update,
    };
    use ta_services::service::TdService;
    use td_common::execution_status::WorkerCallbackStatus;
    use td_common::server::{MessageAction, ResponseMessagePayload};
    use td_common::status::ExitStatus;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDBWithNames;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
    use td_objects::dxo::yaml::WorkerOutput;
    use td_objects::dxo::yaml::function_runner::v2::{FunctionOutput, TableInfo, WrittenTable};
    use td_objects::types::basic::{
        AccessTokenId, CollectionName, ColumnCount, ExecutionStatus, FunctionName, HasData,
        LogicalFunctionRunStatus, ProjectId, RoleId, RowCount, SchemaChanged, SchemaHash,
        TableName, TableNameDto, TransactionStatus, UserId,
    };
    use td_objects::types::composed::TableDependencyDto;
    use td_queries::LiveEntry;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(db)]
    async fn test_tower_metadata_execution_callback(db: TdPool) {
        use td_database::scoped::Scoped;
        use td_objects::types::basic::ProjectId;
        use td_tower::metadata::type_of_val;

        ExecutionCallbackService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<Scoped<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>, ProjectId>, ()>(&[
                // Extract from request.
                type_of_val(&
                    With::<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>>::extract::<
                        RequestContext,
                    >
                ),
                type_of_val(&With::<RequestContext>::extract::<AtTime>),
                type_of_val(&
                    With::<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>>::extract_name::<
                        PhysicalFunctionRunWorkerIdParam,
                    >
                ),
                type_of_val(&
                    With::<UpdateRequest<PhysicalFunctionRunWorkerIdParam, CallbackRequest>>::extract_data::<
                        CallbackRequest,
                    >
                ),
                // Convert callback request to status update request.
                type_of_val(&With::<CallbackRequest>::try_convert_to::<UpdateWorkerExecution, _>),
                // Extract worker_id. We assume it's correct as the callback is constructed by the server.
                type_of_val(&With::<PhysicalFunctionRunWorkerIdParam>::extract::<WorkerId>),
                // Find worker (we will always have 1).
                type_of_val(&By::<WorkerId>::assert_exists::<WorkerDB>),
                // Update worker status.
                type_of_val(&With::<UpdateWorkerExecution>::try_convert_to::<UpdateWorkerDBBuilder, _>),
                type_of_val(&With::<UpdateWorkerDBBuilder>::build::<UpdateWorkerDB>),
                type_of_val(&update_worker_status),
                // Per-attempt audit on the PhysicalFunctionRunDB row.
                type_of_val(&With::<PhysicalFunctionRunWorkerIdParam>::extract::<PhysicalFunctionRunId>),
                type_of_val(&By::<PhysicalFunctionRunId>::fetch_all::<PhysicalFunctionRunDB>),
                type_of_val(&With::<UpdateWorkerExecution>::try_convert_to::<UpdatePhysicalFunctionRunDBBuilder, _>),
                type_of_val(&With::<UpdatePhysicalFunctionRunDBBuilder>::build::<UpdatePhysicalFunctionRunDB>),
                type_of_val(&update_function_run_audit),
                // Canonical flow-control status update on the LFR.
                type_of_val(&resolve_logical_function_runs),
                type_of_val(&With::<UpdateWorkerExecution>::try_convert_to::<UpdateLogicalFunctionRunDBBuilder, _>),
                type_of_val(&With::<UpdateLogicalFunctionRunDBBuilder>::build::<UpdateLogicalFunctionRunDB>),
                type_of_val(&update_function_run_status),
                // Redirect-at-finish for replay attempts.
                type_of_val(&redirect_pointers_at_finish),
                // Update table data versions status.
                type_of_val(&update_table_data_version_status),
                // Notify scheduler of status change.
                type_of_val(&notify_scheduler),
            ]);
    }

    async fn test_callback(
        db: TdPool,
        test_executions: Vec<TestExecution>,
        callback_function: &str,
        callback_status: WorkerCallbackStatus,
        function_output: Option<WorkerOutput>,
    ) -> Result<(), TdError> {
        test_status_update(db.clone(), &test_executions, |_, _, _, f, w| {
            let db = db.clone();
            let callback_status = callback_status.clone();
            let function_output = function_output.clone();
            let test_function = test_executions
                .iter()
                .flat_map(|e| &e.transactions)
                .flat_map(|t| &t.functions)
                .find(|f| f.name.as_str() == callback_function)
                .unwrap();
            let physical_function_run = (*f[&test_function].id).clone();
            let worker = *w[&test_function].id;

            async move {
                let response: CallbackRequest = ResponseMessagePayload {
                    action: MessageAction::Notify,
                    start: Some(123),
                    end: Some(456),
                    status: callback_status,
                    error_message: None,
                    exit_status: ExitStatus::Success.code(),
                    context: function_output,
                };

                let request =
                    RequestContext::with(AccessTokenId::default(), UserId::admin())
                        .update(
                            PhysicalFunctionRunWorkerIdParam {
                                function_run_id: physical_function_run.into(),
                                worker_id: worker.into(),
                            },
                            response,
                        )
                        .in_project(ProjectId::sandbox());

                let service = ExecutionCallbackService::with_defaults(db.clone()).service();
                service.raw_oneshot(request).await
            }
        })
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_running(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Running,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![TableNameDto::try_from("t_0")?],
                        initial_status: LogicalFunctionRunStatus::Requested,
                        expected_status: LogicalFunctionRunStatus::Running,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_running_multiple_function_runs(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Running,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Requested,
                            expected_status: LogicalFunctionRunStatus::Running,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Scheduled,
                        },
                    ],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_running_multiple_transactions(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Running,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Requested,
                            expected_status: LogicalFunctionRunStatus::Running,
                        }],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Scheduled,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Scheduled,
                            expected_status: LogicalFunctionRunStatus::Scheduled,
                        }],
                    },
                ],
            }],
            "f_0",
            WorkerCallbackStatus::Running,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_published(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Committed,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Done,
                            expected_status: LogicalFunctionRunStatus::Committed,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Committed,
                        },
                    ],
                }],
            }],
            "f_1",
            WorkerCallbackStatus::Done,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_published_downstream(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![
                    TestTransaction {
                        expected_status: TransactionStatus::Committed,
                        functions: vec![
                            TestFunction {
                                collection: CollectionName::try_from("c_0")?,
                                name: FunctionName::try_from("f_0")?,
                                dependencies: vec![],
                                tables: vec![TableNameDto::try_from("t_0")?],
                                initial_status: LogicalFunctionRunStatus::Done,
                                expected_status: LogicalFunctionRunStatus::Committed,
                            },
                            TestFunction {
                                collection: CollectionName::try_from("c_0")?,
                                name: FunctionName::try_from("f_1")?,
                                dependencies: vec![TableDependencyDto::try_from("t_0")?],
                                tables: vec![TableNameDto::try_from("t_1")?],
                                initial_status: LogicalFunctionRunStatus::Running,
                                expected_status: LogicalFunctionRunStatus::Committed,
                            },
                        ],
                    },
                    TestTransaction {
                        expected_status: TransactionStatus::Committed,
                        functions: vec![TestFunction {
                            collection: CollectionName::try_from("c_2")?,
                            name: FunctionName::try_from("f_2")?,
                            dependencies: vec![TableDependencyDto::try_from("c_0/t_1")?],
                            tables: vec![TableNameDto::try_from("t_2")?],
                            initial_status: LogicalFunctionRunStatus::Done,
                            expected_status: LogicalFunctionRunStatus::Committed,
                        }],
                    },
                ],
            }],
            "f_1",
            WorkerCallbackStatus::Done,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_done(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Running,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Running,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Done,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Running,
                        },
                    ],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_failed(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Stalled,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Stalled,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Done,
                            expected_status: LogicalFunctionRunStatus::Done,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Failed,
                        },
                    ],
                }],
            }],
            "f_1",
            WorkerCallbackStatus::Failed,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_failed_downstream(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db,
            vec![TestExecution {
                expected_status: ExecutionStatus::Stalled,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Stalled,
                    functions: vec![
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_0")?,
                            dependencies: vec![],
                            tables: vec![TableNameDto::try_from("t_0")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::Failed,
                        },
                        TestFunction {
                            collection: CollectionName::try_from("c_0")?,
                            name: FunctionName::try_from("f_1")?,
                            dependencies: vec![TableDependencyDto::try_from("t_0")?],
                            tables: vec![TableNameDto::try_from("t_1")?],
                            initial_status: LogicalFunctionRunStatus::Running,
                            expected_status: LogicalFunctionRunStatus::OnHold,
                        },
                    ],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Failed,
            None,
        )
        .await
    }

    #[td_test::test(db)]
    async fn test_callback_table_data_version_status(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db.clone(),
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Committed,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![
                            TableNameDto::try_from("t_0")?,
                            TableNameDto::try_from("t_1")?,
                        ],
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(WorkerOutput::FunctionV2(
                FunctionOutput::builder()
                    .output(vec![
                        WrittenTable::NoData {
                            table: TableName::try_from("t_0")?,
                        },
                        WrittenTable::Data {
                            table: TableName::try_from("t_1")?,
                            info: TableInfo::builder()
                                .column_count(ColumnCount::try_from(1i64)?)
                                .row_count(RowCount::try_from(2i64)?)
                                .schema_hash(SchemaHash::try_from("hash")?)
                                .build()
                                .unwrap(),
                        },
                    ])
                    .build()?,
            )),
        )
        .await?;

        // Assertions
        let physical_function_runs: Vec<PhysicalFunctionRunDBWithNames> = db
            .live()
            .fetch_all::<PhysicalFunctionRunDBWithNames>(&FunctionName::try_from("f_0")?)
            .await?;
        assert_eq!(physical_function_runs.len(), 1);
        let physical_function_run = &physical_function_runs[0];

        let physical_table_data_versions: Vec<PhysicalTableDataVersionDBWithNames> = db
            .live()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_0")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 1);
        let physical_table_data_version = &physical_table_data_versions[0];
        assert_eq!(
            physical_table_data_version.triggered_on,
            physical_function_run.triggered_on
        );
        assert_eq!(
            physical_table_data_version.triggered_by_id,
            physical_function_run.triggered_by_id
        );
        assert_eq!(
            physical_table_data_version.status,
            physical_function_run.status
        );
        assert_eq!(physical_table_data_version.has_data, Some(HasData::FALSE));
        assert_eq!(
            physical_table_data_version.schema_changed,
            Some(SchemaChanged::FALSE)
        );

        let physical_table_data_versions: Vec<PhysicalTableDataVersionDBWithNames> = db
            .live()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_1")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 1);
        let physical_table_data_version = &physical_table_data_versions[0];
        assert_eq!(
            physical_table_data_version.triggered_on,
            physical_function_run.triggered_on
        );
        assert_eq!(
            physical_table_data_version.triggered_by_id,
            physical_function_run.triggered_by_id
        );
        assert_eq!(
            physical_table_data_version.status,
            physical_function_run.status
        );
        assert_eq!(physical_table_data_version.has_data, Some(HasData::TRUE));
        assert_eq!(
            physical_table_data_version.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            physical_table_data_version.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            physical_table_data_version.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            physical_table_data_version.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );

        Ok(())
    }

    #[td_test::test(db)]
    async fn test_callback_table_data_version_data_information(db: TdPool) -> Result<(), TdError> {
        test_callback(
            db.clone(),
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Committed,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![
                            TableNameDto::try_from("t_0")?,
                            TableNameDto::try_from("t_1")?,
                        ],
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(WorkerOutput::FunctionV2(
                FunctionOutput::builder()
                    .output(vec![
                        WrittenTable::NoData {
                            table: TableName::try_from("t_0")?,
                        },
                        WrittenTable::Data {
                            table: TableName::try_from("t_1")?,
                            info: TableInfo::builder()
                                .column_count(ColumnCount::try_from(1i64)?)
                                .row_count(RowCount::try_from(2i64)?)
                                .schema_hash(SchemaHash::try_from("hash")?)
                                .build()
                                .unwrap(),
                        },
                    ])
                    .build()?,
            )),
        )
        .await?;

        test_callback(
            db.clone(),
            vec![TestExecution {
                expected_status: ExecutionStatus::Finished,
                transactions: vec![TestTransaction {
                    expected_status: TransactionStatus::Committed,
                    functions: vec![TestFunction {
                        collection: CollectionName::try_from("c_0")?,
                        name: FunctionName::try_from("f_0")?,
                        dependencies: vec![],
                        tables: vec![
                            TableNameDto::try_from("t_0")?,
                            TableNameDto::try_from("t_1")?,
                        ],
                        initial_status: LogicalFunctionRunStatus::Running,
                        expected_status: LogicalFunctionRunStatus::Committed,
                    }],
                }],
            }],
            "f_0",
            WorkerCallbackStatus::Done,
            Some(WorkerOutput::FunctionV2(
                FunctionOutput::builder()
                    .output(vec![
                        WrittenTable::Data {
                            table: TableName::try_from("t_0")?,
                            info: TableInfo::builder()
                                .column_count(ColumnCount::try_from(3i64)?)
                                .row_count(RowCount::try_from(4i64)?)
                                .schema_hash(SchemaHash::try_from("hash")?)
                                .build()
                                .unwrap(),
                        },
                        WrittenTable::NoData {
                            table: TableName::try_from("t_1")?,
                        },
                    ])
                    .build()?,
            )),
        )
        .await?;

        // Assertions
        let physical_function_runs = db
            .live()
            .fetch_all::<PhysicalFunctionRunDBWithNames>(&FunctionName::try_from("f_0")?)
            .await?;
        assert_eq!(physical_function_runs.len(), 2);
        let function_run_0 = &physical_function_runs[0];
        let function_run_1 = &physical_function_runs[1];

        // t_0
        let physical_table_data_versions = db
            .live()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_0")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 2);
        let table_data_version_0 = &physical_table_data_versions[0];
        let table_data_version_1 = &physical_table_data_versions[1];

        assert_eq!(
            table_data_version_0.triggered_on,
            function_run_0.triggered_on
        );
        assert_eq!(
            table_data_version_1.triggered_on,
            function_run_1.triggered_on
        );
        assert_eq!(
            table_data_version_0.triggered_by_id,
            function_run_0.triggered_by_id
        );
        assert_eq!(
            table_data_version_1.triggered_by_id,
            function_run_1.triggered_by_id
        );
        assert_eq!(table_data_version_0.status, function_run_0.status);
        assert_eq!(table_data_version_1.status, function_run_1.status);

        // version_0 (newer) has data, and version_1 (older) does not
        assert_eq!(table_data_version_0.has_data, Some(HasData::TRUE));
        assert_eq!(
            table_data_version_0.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_0.column_count,
            Some(ColumnCount::try_from(3i64)?)
        );
        assert_eq!(
            table_data_version_0.row_count,
            Some(RowCount::try_from(4i64)?)
        );
        assert_eq!(
            table_data_version_0.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        assert_eq!(table_data_version_1.has_data, Some(HasData::FALSE));
        assert_eq!(
            table_data_version_1.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(table_data_version_1.column_count, None);
        assert_eq!(table_data_version_1.row_count, None);
        assert_eq!(table_data_version_1.schema_hash, None);

        // t_1
        let physical_table_data_versions = db
            .live()
            .fetch_all::<PhysicalTableDataVersionDBWithNames>(&TableName::try_from("t_1")?)
            .await?;
        assert_eq!(physical_table_data_versions.len(), 2);
        let table_data_version_0 = &physical_table_data_versions[0];
        let table_data_version_1 = &physical_table_data_versions[1];

        assert_eq!(
            table_data_version_0.triggered_on,
            function_run_0.triggered_on
        );
        assert_eq!(
            table_data_version_1.triggered_on,
            function_run_1.triggered_on
        );
        assert_eq!(
            table_data_version_0.triggered_by_id,
            function_run_0.triggered_by_id
        );
        assert_eq!(
            table_data_version_1.triggered_by_id,
            function_run_1.triggered_by_id
        );
        assert_eq!(table_data_version_0.status, function_run_0.status);
        assert_eq!(table_data_version_1.status, function_run_1.status);

        // version_0 (newer) has no data, and version_1 (older) does. version_0 inherits data info
        assert_eq!(table_data_version_0.has_data, Some(HasData::FALSE));
        assert_eq!(
            table_data_version_0.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_0.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version_0.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version_0.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        assert_eq!(table_data_version_1.has_data, Some(HasData::TRUE));
        assert_eq!(
            table_data_version_1.schema_changed,
            Some(SchemaChanged::FALSE)
        );
        assert_eq!(
            table_data_version_1.column_count,
            Some(ColumnCount::try_from(1i64)?)
        );
        assert_eq!(
            table_data_version_1.row_count,
            Some(RowCount::try_from(2i64)?)
        );
        assert_eq!(
            table_data_version_1.schema_hash,
            Some(SchemaHash::try_from("hash")?)
        );
        Ok(())
    }
}
