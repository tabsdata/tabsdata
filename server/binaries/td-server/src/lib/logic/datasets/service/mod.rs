//
//  Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::service::create_dataset::CreateDatasetService;
use crate::logic::datasets::service::data::DataService;
use crate::logic::datasets::service::execution::cancel::CancelExecutionService;
use crate::logic::datasets::service::execution::create_plan::CreatePlanService;
use crate::logic::datasets::service::execution::list_worker_messages::ListWorkerMessagesService;
use crate::logic::datasets::service::execution::read_plan::ReadPlanService;
use crate::logic::datasets::service::execution::read_worker_logs::ReadWorkerLogsService;
use crate::logic::datasets::service::execution::recover::RecoverExecutionService;
use crate::logic::datasets::service::execution::template::TemplateService;
use crate::logic::datasets::service::execution::update_status::UpdateExecutionStatusService;
use crate::logic::datasets::service::list_commits::ListCommitsService;
use crate::logic::datasets::service::list_dataset_functions::ListDatasetFunctionsService;
use crate::logic::datasets::service::list_dataset_versions::ListDatasetVersionsService;
use crate::logic::datasets::service::list_datasets::ListDatasetsService;
use crate::logic::datasets::service::list_execution_plans::ListExecutionPlansService;
use crate::logic::datasets::service::list_tables::ListTablesService;
use crate::logic::datasets::service::list_transactions::ListTransactionsService;
use crate::logic::datasets::service::read_dataset::ReadDatasetService;
use crate::logic::datasets::service::sample::SampleService;
use crate::logic::datasets::service::schema::SchemaService;
use crate::logic::datasets::service::update_dataset::UpdateDatasetService;
use crate::logic::datasets::service::upload_function::UploadFunctionService;
use std::sync::Arc;
use td_common::execution_status::DataVersionUpdateRequest;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest};
use td_objects::datasets::dlo::BoxedSyncStream;
use td_objects::datasets::dto::{
    CommitList, DataVersionList, DatasetList, DatasetRead, DatasetWrite, ExecutionPlanList,
    ExecutionPlanRead, ExecutionPlanWrite, ExecutionTemplateRead, FunctionList, SchemaField,
    TableList, TransactionList, UploadFunction, WorkerMessageList,
};
use td_objects::dlo::{CollectionName, DataVersionId, TransactionId};
use td_objects::rest_urls::{
    CollectionParam, ExecutionPlanIdParam, FunctionParam, TableCommitParam, WorkerMessageListParam,
    WorkerMessageParam,
};
use td_storage::{SPath, Storage};
use td_tower::service_provider::TdBoxService;
use td_transaction::TransactionBy;

pub mod create_dataset;
pub mod data;
pub mod execution;
pub mod list_commits;
pub mod list_dataset_functions;
pub mod list_dataset_versions;
pub mod list_datasets;
pub mod list_execution_plans;
pub mod list_tables;
pub mod list_transactions;
pub mod read_dataset;
pub mod sample;
pub mod schema;
pub mod update_dataset;
pub mod upload_function;

#[cfg(test)]
mod test_errors;

pub struct DatasetServices {
    create_service_provider: CreateDatasetService,
    update_service_provider: UpdateDatasetService,
    read_service_provider: ReadDatasetService,
    upload_function_provider: UploadFunctionService,
    list_service_provider: ListDatasetsService,
    list_dataset_functions_provider: ListDatasetFunctionsService,
    template_service_provider: TemplateService,
    create_plan_service_provider: CreatePlanService,
    read_plan_service_provider: ReadPlanService,
    update_execution_status_service_provider: UpdateExecutionStatusService,
    recover_execution_service_provider: RecoverExecutionService,
    cancel_execution_service_provider: CancelExecutionService,
    list_dataset_versions_service: ListDatasetVersionsService,
    list_execution_plans_service: ListExecutionPlansService,
    data_service: DataService,
    schema_service: SchemaService,
    sample_service: SampleService,
    list_tables_service: ListTablesService,
    list_transactions_service: ListTransactionsService,
    list_commits_service: ListCommitsService,
    list_worker_messages_service: ListWorkerMessagesService,
    read_worker_service: ReadWorkerLogsService,
}

impl DatasetServices {
    pub fn new(db: DbPool, storage: Arc<Storage>, transaction_by: Arc<TransactionBy>) -> Self {
        Self {
            create_service_provider: CreateDatasetService::new(db.clone()),
            update_service_provider: UpdateDatasetService::new(db.clone()),
            read_service_provider: ReadDatasetService::new(db.clone()),
            upload_function_provider: UploadFunctionService::new(db.clone(), storage.clone()),
            list_service_provider: ListDatasetsService::new(db.clone()),
            list_dataset_functions_provider: ListDatasetFunctionsService::new(db.clone()),
            template_service_provider: TemplateService::new(db.clone(), transaction_by.clone()),
            create_plan_service_provider: CreatePlanService::new(
                db.clone(),
                transaction_by.clone(),
            ),
            read_plan_service_provider: ReadPlanService::new(db.clone(), transaction_by.clone()),
            update_execution_status_service_provider: UpdateExecutionStatusService::new(db.clone()),
            recover_execution_service_provider: RecoverExecutionService::new(db.clone()),
            cancel_execution_service_provider: CancelExecutionService::new(db.clone()),
            list_dataset_versions_service: ListDatasetVersionsService::new(db.clone()),
            list_execution_plans_service: ListExecutionPlansService::new(db.clone()),
            data_service: DataService::new(db.clone()),
            schema_service: SchemaService::new(db.clone(), storage.clone()),
            sample_service: SampleService::new(db.clone(), storage.clone()),
            list_tables_service: ListTablesService::new(db.clone()),
            list_transactions_service: ListTransactionsService::new(db.clone()),
            list_commits_service: ListCommitsService::new(db.clone()),
            list_worker_messages_service: ListWorkerMessagesService::new(db.clone()),
            read_worker_service: ReadWorkerLogsService::new(db.clone()),
        }
    }

    pub async fn create_dataset(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionName, DatasetWrite>, DatasetRead, TdError> {
        self.create_service_provider.service().await
    }

    pub async fn update_dataset(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionParam, DatasetWrite>, DatasetRead, TdError> {
        self.update_service_provider.service().await
    }

    pub async fn read_dataset(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, DatasetRead, TdError> {
        self.read_service_provider.service().await
    }

    pub async fn upload_function(&self) -> TdBoxService<UploadFunction, (), TdError> {
        self.upload_function_provider.service().await
    }

    pub async fn list_datasets(
        &self,
    ) -> TdBoxService<ListRequest<CollectionName>, ListResponse<DatasetList>, TdError> {
        self.list_service_provider.service().await
    }

    pub async fn list_dataset_functions(
        &self,
    ) -> TdBoxService<ListRequest<FunctionParam>, ListResponse<FunctionList>, TdError> {
        self.list_dataset_functions_provider.service().await
    }

    pub async fn create_execution_template(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, ExecutionTemplateRead, TdError> {
        self.template_service_provider.service().await
    }

    pub async fn create_execution_plan(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, ExecutionPlanWrite>, ExecutionPlanRead, TdError>
    {
        self.create_plan_service_provider.service().await
    }

    pub async fn read_execution_plan(
        &self,
    ) -> TdBoxService<ReadRequest<ExecutionPlanIdParam>, ExecutionPlanRead, TdError> {
        self.read_plan_service_provider.service().await
    }

    pub async fn update_execution_status(
        &self,
    ) -> TdBoxService<UpdateRequest<DataVersionId, DataVersionUpdateRequest>, (), TdError> {
        self.update_execution_status_service_provider
            .service()
            .await
    }

    pub async fn recover_execution(
        &self,
    ) -> TdBoxService<UpdateRequest<TransactionId, ()>, (), TdError> {
        self.recover_execution_service_provider.service().await
    }

    pub async fn cancel_execution(
        &self,
    ) -> TdBoxService<UpdateRequest<TransactionId, ()>, (), TdError> {
        self.cancel_execution_service_provider.service().await
    }

    pub async fn list_dataset_versions(
        &self,
    ) -> TdBoxService<ListRequest<FunctionParam>, ListResponse<DataVersionList>, TdError> {
        self.list_dataset_versions_service.service().await
    }

    pub async fn list_execution_plans(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<ExecutionPlanList>, TdError> {
        self.list_execution_plans_service.service().await
    }

    pub async fn data(&self) -> TdBoxService<ReadRequest<TableCommitParam>, SPath, TdError> {
        self.data_service.service().await
    }

    pub async fn schema(
        &self,
    ) -> TdBoxService<ReadRequest<TableCommitParam>, Vec<SchemaField>, TdError> {
        self.schema_service.service().await
    }

    pub async fn sample(
        &self,
    ) -> TdBoxService<ListRequest<TableCommitParam>, BoxedSyncStream, TdError> {
        self.sample_service.service().await
    }

    pub async fn list_tables(
        &self,
    ) -> TdBoxService<ListRequest<CollectionParam>, ListResponse<TableList>, TdError> {
        self.list_tables_service.service().await
    }

    pub async fn list_transactions(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<TransactionList>, TdError> {
        self.list_transactions_service.service().await
    }

    pub async fn list_commits(
        &self,
    ) -> TdBoxService<ListRequest<()>, ListResponse<CommitList>, TdError> {
        self.list_commits_service.service().await
    }

    pub async fn list_worker_messages(
        &self,
    ) -> TdBoxService<ListRequest<WorkerMessageListParam>, ListResponse<WorkerMessageList>, TdError>
    {
        self.list_worker_messages_service.service().await
    }

    pub async fn read_worker(
        &self,
    ) -> TdBoxService<ReadRequest<WorkerMessageParam>, BoxedSyncStream, TdError> {
        self.read_worker_service.service().await
    }
}
