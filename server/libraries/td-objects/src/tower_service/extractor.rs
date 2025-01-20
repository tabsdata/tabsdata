//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{
    CreateRequest, DeleteRequest, ListParams, ListRequest, Name, ReadRequest, RequestContext,
    UpdateRequest,
};
use crate::dlo::{
    CollectionId, CollectionName, Creator, DataVersionId, DatasetId, DatasetName, ExecutionPlanId,
    FunctionId, RequestIsAdmin, RequestTime, RequestUserId, TransactionId, UserId, UserName,
    WorkerMessageId,
};
use crate::rest_urls::WorkerMessageParam;
use chrono::{DateTime, Utc};
use td_common::error::TdError;
use td_common::server::SupervisorMessage;
use td_common::uri::Version;
use td_tower::extractors::Input;

pub trait UserNameProvider {
    fn user_name(&self) -> &str;
}

pub async fn extract_user_name<P: UserNameProvider>(
    Input(provider): Input<P>,
) -> Result<UserName, TdError> {
    Ok(UserName::new(provider.user_name()))
}

pub trait UserIdProvider {
    fn user_id(&self) -> String;
}

pub trait RequestUserIdProvider {
    fn request_user_id(&self) -> String;
}

pub async fn extract_user_id<P: UserIdProvider>(
    Input(provider): Input<P>,
) -> Result<UserId, TdError> {
    Ok(UserId::new(provider.user_id()))
}

pub async fn extract_req_user_id<P: RequestUserIdProvider>(
    Input(provider): Input<P>,
) -> Result<RequestUserId, TdError> {
    Ok(RequestUserId::new(provider.request_user_id()))
}

pub trait CollectionNameProvider {
    fn collection_name(&self) -> String;
}

pub async fn extract_collection_name<P: CollectionNameProvider>(
    Input(provider): Input<P>,
) -> Result<CollectionName, TdError> {
    Ok(CollectionName::new(provider.collection_name()))
}

pub trait CollectionIdProvider {
    fn collection_id(&self) -> String;
}

pub async fn extract_collection_id<P: CollectionIdProvider>(
    Input(provider): Input<P>,
) -> Result<CollectionId, TdError> {
    Ok(CollectionId::new(provider.collection_id()))
}

pub trait DatasetNameProvider {
    fn dataset_name(&self) -> String;
}

pub async fn extract_dataset_name<P: DatasetNameProvider>(
    Input(provider): Input<P>,
) -> Result<DatasetName, TdError> {
    Ok(DatasetName::new(provider.dataset_name()))
}

pub trait VersionProvider {
    fn version(&self) -> String;
}

pub async fn extract_version<P: VersionProvider>(
    Input(provider): Input<P>,
) -> Result<Version, TdError> {
    Ok(Version::parse(&provider.version())?)
}

pub trait TableProvider {
    fn table(&self) -> String;
}

pub async fn extract_table<P: TableProvider>(
    Input(provider): Input<P>,
) -> Result<Name<String>, TdError> {
    Ok(Name::new(provider.table()))
}

pub trait DatasetIdProvider {
    fn dataset_id(&self) -> String;
}

pub async fn extract_dataset_id<P: DatasetIdProvider>(
    Input(provider): Input<P>,
) -> Result<DatasetId, TdError> {
    Ok(DatasetId::new(provider.dataset_id()))
}

pub trait FunctionIdProvider {
    fn function_id(&self) -> String;
}

pub async fn extract_function_id<P: FunctionIdProvider>(
    Input(provider): Input<P>,
) -> Result<FunctionId, TdError> {
    Ok(FunctionId::new(provider.function_id()))
}

pub trait DataVersionIdProvider {
    fn data_version_id(&self) -> String;
}

pub async fn extract_data_version_id<P: DataVersionIdProvider>(
    Input(provider): Input<P>,
) -> Result<DataVersionId, TdError> {
    Ok(DataVersionId::new(provider.data_version_id()))
}

pub trait ExecutionPlanIdProvider {
    fn execution_plan_id(&self) -> String;
}

pub async fn extract_execution_plan_id<P: ExecutionPlanIdProvider>(
    Input(provider): Input<P>,
) -> Result<ExecutionPlanId, TdError> {
    Ok(ExecutionPlanId::new(provider.execution_plan_id()))
}

pub trait TransactionIdProvider {
    fn transaction_id(&self) -> String;
}

pub async fn extract_transaction_id<P: TransactionIdProvider>(
    Input(provider): Input<P>,
) -> Result<TransactionId, TdError> {
    Ok(TransactionId::new(provider.transaction_id()))
}

pub async fn to_vec<P: Clone>(Input(provider): Input<P>) -> Result<Vec<P>, TdError> {
    let cloned = (*provider).clone();
    Ok(vec![cloned])
}

pub trait RequestContextProvider {
    fn context(&self) -> RequestContext;
}

impl<N, C> RequestContextProvider for CreateRequest<N, C> {
    fn context(&self) -> RequestContext {
        self.context().clone()
    }
}

impl<C, N> RequestContextProvider for UpdateRequest<C, N> {
    fn context(&self) -> RequestContext {
        self.context().clone()
    }
}

impl<N> RequestContextProvider for ReadRequest<N> {
    fn context(&self) -> RequestContext {
        self.context().clone()
    }
}

impl<N> RequestContextProvider for DeleteRequest<N> {
    fn context(&self) -> RequestContext {
        self.context().clone()
    }
}

impl<N> RequestContextProvider for ListRequest<N> {
    fn context(&self) -> RequestContext {
        self.context().clone()
    }
}

pub async fn extract_req_context<C: RequestContextProvider>(
    Input(provider): Input<C>,
) -> Result<RequestContext, TdError> {
    Ok(provider.context())
}

pub trait IsAdminRequestProvider {
    fn is_admin(&self) -> bool;
}

impl<C: RequestContextProvider> IsAdminRequestProvider for C {
    fn is_admin(&self) -> bool {
        *self.context().sys_admin()
    }
}

pub async fn extract_req_is_admin<P: IsAdminRequestProvider>(
    Input(provider): Input<P>,
) -> Result<RequestIsAdmin, TdError> {
    Ok(RequestIsAdmin(provider.is_admin()))
}

pub trait TimeProvider {
    fn time(&self) -> DateTime<Utc>;
}

impl<C: RequestContextProvider> TimeProvider for C {
    fn time(&self) -> DateTime<Utc> {
        *self.context().time()
    }
}

pub async fn extract_req_time<P: TimeProvider>(
    Input(provider): Input<P>,
) -> Result<RequestTime, TdError> {
    Ok(RequestTime(provider.time()))
}

impl<C: RequestContextProvider> RequestUserIdProvider for C {
    fn request_user_id(&self) -> String {
        self.context().user_id().to_string()
    }
}

pub trait RequestDtoProvider<D, N> {
    fn dto(&self) -> D;
}

impl<C: Clone, N> RequestDtoProvider<C, N> for CreateRequest<N, C> {
    fn dto(&self) -> C {
        self.data().clone()
    }
}

impl<C: Clone, N> RequestDtoProvider<C, N> for UpdateRequest<N, C> {
    fn dto(&self) -> C {
        self.data().clone()
    }
}

pub async fn extract_req_dto<P: RequestDtoProvider<D, N>, N, D>(
    Input(provider): Input<P>,
) -> Result<D, TdError> {
    Ok(provider.dto())
}

pub trait RequestNameProvider<N> {
    fn name(&self) -> Name<N>;
}

impl<C, N: Clone> RequestNameProvider<N> for CreateRequest<N, C> {
    fn name(&self) -> Name<N> {
        self.name().clone()
    }
}

impl<C, N: Clone> RequestNameProvider<N> for UpdateRequest<N, C> {
    fn name(&self) -> Name<N> {
        self.name().clone()
    }
}

impl<N: Clone> RequestNameProvider<N> for ReadRequest<N> {
    fn name(&self) -> Name<N> {
        self.name().clone()
    }
}

impl<N: Clone> RequestNameProvider<N> for DeleteRequest<N> {
    fn name(&self) -> Name<N> {
        self.name().clone()
    }
}

impl<N: Clone> RequestNameProvider<N> for ListRequest<N> {
    fn name(&self) -> Name<N> {
        self.name().clone()
    }
}

pub async fn extract_req_name<P: RequestNameProvider<N>, N: Clone>(
    Input(provider): Input<P>,
) -> Result<N, TdError> {
    Ok(provider.name().value().clone())
}

pub async fn extract_name<P: RequestNameProvider<N>, N: for<'a> From<&'a N>, C: Creator<N>>(
    Input(provider): Input<P>,
) -> Result<C, TdError> {
    Ok(C::create(provider.name().value()))
}

pub trait ListParamsProvider {
    fn list_params(&self) -> ListParams;
}

impl<N: Clone> ListParamsProvider for ListRequest<N> {
    fn list_params(&self) -> ListParams {
        self.list_params().clone()
    }
}

pub async fn extract_list_params<P: ListParamsProvider>(
    Input(provider): Input<P>,
) -> Result<ListParams, TdError> {
    Ok(provider.list_params())
}

pub trait WorkerIdProvider {
    fn worker_message_id(&self) -> WorkerMessageId;
}

impl<F: Clone> WorkerIdProvider for SupervisorMessage<F> {
    fn worker_message_id(&self) -> WorkerMessageId {
        WorkerMessageId(self.id().clone())
    }
}

pub async fn extract_message_id<P: WorkerIdProvider>(
    Input(provider): Input<P>,
) -> Result<WorkerMessageId, TdError> {
    Ok(provider.worker_message_id())
}

impl WorkerIdProvider for WorkerMessageParam {
    fn worker_message_id(&self) -> WorkerMessageId {
        WorkerMessageId(self.worker_id().clone())
    }
}
