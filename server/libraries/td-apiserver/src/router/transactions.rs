//
// Copyright 2025 Tabs Data Inc.
//

use td_apiforge::router_ext;

#[router_ext(TransactionsRouter)]
mod routes {
    use axum::Extension;
    use axum::extract::{Path, State};
    use axum_extra::extract::Query;
    use std::sync::Arc;
    use ta_apiserver::status::error_status::ErrorStatus;
    use ta_apiserver::status::ok_status::{ListStatus, NoContent, UpdateStatus};
    use ta_services::service::TdService;
    use td_apiforge::apiserver_path;
    use td_objects::dxo::crudl::{ListParams, RequestContext};
    use td_objects::dxo::synchrotron::defs::SynchrotronResponse;
    use td_objects::dxo::transaction::defs::Transaction;
    use td_objects::rest_urls::{
        SYNCHROTRON_READ, TRANSACTION_CANCEL, TRANSACTION_RECOVER, TRANSACTIONS_LIST,
        TransactionParam,
    };
    use td_services::transaction::services::TransactionServices;
    use tower::ServiceExt;

    const TRANSACTIONS_TAG: &str = "Transactions";

    #[apiserver_path(method = post, path = TRANSACTION_CANCEL, tag = TRANSACTIONS_TAG)]
    #[doc = "Cancel all function runs in the given transaction"]
    pub async fn cancel(
        State(transaction): State<Arc<TransactionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<TransactionParam>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, ());
        let response = transaction.cancel.service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = get, path = TRANSACTIONS_LIST, tag = TRANSACTIONS_TAG)]
    #[doc = "List transactions"]
    pub async fn list(
        State(transaction): State<Arc<TransactionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<Transaction>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = transaction.list.service().await.oneshot(request).await?;
        Ok(ListStatus::OK(response))
    }

    #[apiserver_path(method = post, path = TRANSACTION_RECOVER, tag = TRANSACTIONS_TAG)]
    #[doc = "Recover all function runs in the given transaction"]
    pub async fn recover(
        State(transaction): State<Arc<TransactionServices>>,
        Extension(context): Extension<RequestContext>,
        Path(param): Path<TransactionParam>,
    ) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
        let request = context.update(param, ());
        let response = transaction.recover.service().await.oneshot(request).await?;
        Ok(UpdateStatus::OK(response))
    }

    #[apiserver_path(method = get, path = SYNCHROTRON_READ, tag = TRANSACTIONS_TAG)]
    #[doc = "Synchrotron endpoint to list transactions in the system"]
    pub async fn synchrotron(
        State(transaction): State<Arc<TransactionServices>>,
        Extension(context): Extension<RequestContext>,
        Query(query_params): Query<ListParams>,
    ) -> Result<ListStatus<SynchrotronResponse>, ErrorStatus> {
        let request = context.list((), query_params);
        let response = transaction
            .synchrotron
            .service()
            .await
            .oneshot(request)
            .await?;
        Ok(ListStatus::OK(response))
    }
}
