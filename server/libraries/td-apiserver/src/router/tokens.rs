//
// Copyright 2026 Tabsdata Inc.
//

//! Lifecycle routes are session-only, so a token cannot mint its successor;
//! `/auth/service-token` answers about the presenting token, so only a token may ask.
//! `/users/{user}/service-tokens` is the administrator's view of another account's
//! slots.

const TOKENS_TAG: &str = "Service Tokens";

use axum::Extension;
use axum::extract::{Path, State};
use axum_extra::extract::Query;
use ta_apiserver::status::error_status::ErrorStatus;
use ta_apiserver::status::extractors::Json;
use ta_apiserver::status::ok_status::{
    CreateStatus, DeleteStatus, GetStatus, ListStatus, NoContent,
};
use td_apiforge::apiserver_path;
use td_objects::dxo::crudl::{ListParams, RequestContext};
use td_objects::dxo::service_token::{
    TokenCreate, TokenCreated, TokenRead, TokenSelf, TokenSlotParam, UserSlotParam,
};
use td_objects::dxo::user::UserParam;
use td_services::Services;

#[apiserver_path(
    version(v1),
    auth(session),
    method(post),
    path = "/auth/service-tokens/{slot}",
    tag(TOKENS_TAG),
    doc = "Creates a service token in one of the caller's two slots"
)]
pub async fn create_service_token(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(slot): Path<TokenSlotParam>,
    Json(request): Json<TokenCreate>,
) -> Result<CreateStatus<TokenCreated>, ErrorStatus> {
    let request = context.create(slot, request);
    let response = state.create_service_token(request).await?;
    Ok(CreateStatus::CREATED(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/auth/service-tokens",
    tag(TOKENS_TAG),
    doc = "Lists the caller's service tokens"
)]
pub async fn list_service_tokens(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<TokenRead>, ErrorStatus> {
    let request = context.list((), list_params);
    let response = state.list_service_tokens(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(delete),
    path = "/auth/service-tokens/{slot}",
    tag(TOKENS_TAG),
    doc = "Revokes the service token in one of the caller's slots"
)]
pub async fn revoke_service_token(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(slot): Path<TokenSlotParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(slot);
    let response = state.revoke_service_token(request).await?;
    Ok(DeleteStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(service_token),
    method(get),
    path = "/auth/service-token",
    tag(TOKENS_TAG),
    doc = "The service token making this request"
)]
pub async fn read_own_service_token(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
) -> Result<GetStatus<TokenSelf>, ErrorStatus> {
    let response = state.read_own_service_token(context).await?;
    Ok(GetStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/users/{user}/service-tokens",
    tag(TOKENS_TAG),
    doc = "Lists a user's service tokens (requires manage_access)"
)]
pub async fn list_user_service_tokens(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(user): Path<UserParam>,
    Query(list_params): Query<ListParams>,
) -> Result<ListStatus<TokenRead>, ErrorStatus> {
    let request = context.list(user, list_params);
    let response = state.list_user_service_tokens(request).await?;
    Ok(ListStatus::OK(response))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(delete),
    path = "/users/{user}/service-tokens/{slot}",
    tag(TOKENS_TAG),
    doc = "Revokes the service token in one of a user's slots (requires manage_access)"
)]
pub async fn revoke_user_service_token(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Path(target): Path<UserSlotParam>,
) -> Result<DeleteStatus<NoContent>, ErrorStatus> {
    let request = context.delete(target);
    let response = state.revoke_user_service_token(request).await?;
    Ok(DeleteStatus::OK(response))
}
