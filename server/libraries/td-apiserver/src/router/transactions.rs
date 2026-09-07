//
// Copyright 2025 Tabsdata Inc.
//

//! Transaction routes, plus the project-scoped synchrotron read. Transactions
//! are top-level: their ids are globally unique, so the services resolve the
//! owning project from the row.

use crate::extractors::Project;
use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::ok_status::{ListStatus, NoContent, UpdateStatus};
use td_apiforge::apiserver_path;
use td_database::pool::scoped::ScopedExt;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::synchrotron::SynchrotronResponse;
use td_objects::dxo::transaction::{Transaction, TransactionParam};
use td_services::Services;

const TRANSACTIONS_TAG: &str = "Transactions";

#[apiserver_path(
    mount(global),
    method(post),
    path = "/transactions/{transaction}/cancel",
    tag(TRANSACTIONS_TAG),
    doc = "Cancel all function runs in the given transaction"
)]
pub async fn cancel_transaction(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(transaction): Path<TransactionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(transaction, ());
    let response = state.transaction_cancel(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    mount(global),
    method(get),
    path = "/transactions",
    tag(TRANSACTIONS_TAG),
    doc = "List transactions"
)]
pub async fn list_transactions(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<Transaction>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.transaction_list(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    mount(global),
    method(post),
    path = "/transactions/{transaction}/recover",
    tag(TRANSACTIONS_TAG),
    doc = "Recover all function runs in the given transaction"
)]
pub async fn recover_transaction(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(transaction): Path<TransactionParam>,
) -> Result<UpdateStatus<NoContent>, ErrorStatus> {
    let request = context.update(transaction, ());
    let response = state.transaction_recover(request).await?;
    Ok(UpdateStatus::OK(response))
}

#[apiserver_path(
    mount(project),
    method(get),
    path = "/projects/{project}/synchrotron",
    tag(TRANSACTIONS_TAG),
    doc = "Synchrotron endpoint to list transactions in the system"
)]
pub async fn synchrotron(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Project(project_id): Project,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<SynchrotronResponse>, ErrorStatus> {
    let request = context.list((), list_params).in_project(project_id);
    let response = state.synchrotron(request).await?;
    Ok(ListStatus::OK(response))
}
