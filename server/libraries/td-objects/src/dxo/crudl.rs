//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::{AccessTokenId, AtTime, UserId};
use serde_valid::Validate;
use std::fmt::Debug;
use td_error::td_error;

/// How the requester was authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Provenance {
    /// A user session (access token) — subject to RBAC.
    #[default]
    Session,
    /// A validated internal-endpoint token: the token itself binds
    /// `(method, scope, bindings)`, so RBAC does not apply.
    InternalToken,
}

/// Identity of the caller making a request: who and when. Carries no project —
/// project scope is a separate, explicit value attached to the request via
/// [`CreateRequest::in_project`] and friends. Carries no role either:
/// effective permissions are the union across all the user's assigned roles,
/// so there is no per-session "active role".
#[ta_objects::Dlo]
#[derive(Copy)]
pub struct RequestContext {
    pub access_token_id: AccessTokenId,
    pub user_id: UserId,
    pub time: AtTime,
    pub provenance: Provenance,
}

impl RequestContext {
    pub fn with(access_token_id: impl Into<AccessTokenId>, user_id: impl Into<UserId>) -> Self {
        Self {
            access_token_id: access_token_id.into(),
            user_id: user_id.into(),
            time: AtTime::default(),
            provenance: Provenance::Session,
        }
    }

    /// The system context injected by the apiserver's internal-token layer
    /// once it has validated an internal-endpoint token against the request.
    pub fn internal() -> Self {
        Self {
            access_token_id: AccessTokenId::default(),
            user_id: UserId::system(),
            time: AtTime::default(),
            provenance: Provenance::InternalToken,
        }
    }
}

#[ta_objects::Dlo]
pub struct CreateRequest<C, N = ()> {
    pub context: RequestContext,
    pub name: N,
    pub data: C,
}

impl<C, N> CreateRequest<C, N> {
    pub fn into_parts(self) -> (RequestContext, N, C) {
        (self.context, self.name, self.data)
    }
}

#[ta_objects::Dlo]
pub struct UpdateRequest<U, N = ()> {
    pub context: RequestContext,
    pub name: N,
    pub data: U,
}

impl<U, N> UpdateRequest<U, N> {
    pub fn into_parts(self) -> (RequestContext, N, U) {
        (self.context, self.name, self.data)
    }
}

#[ta_objects::Dlo]
pub struct DeleteRequest<N = ()> {
    pub context: RequestContext,
    pub name: N,
}

impl<N> DeleteRequest<N> {
    pub fn into_parts(self) -> (RequestContext, N) {
        (self.context, self.name)
    }
}

#[ta_objects::Dlo]
pub struct ReadRequest<N = ()> {
    pub context: RequestContext,
    pub name: N,
}

impl<N> ReadRequest<N> {
    pub fn into_parts(self) -> (RequestContext, N) {
        (self.context, self.name)
    }
}

const DEFAULT_PAGE_LEN: usize = 50;

fn default_page_len() -> usize {
    DEFAULT_PAGE_LEN
}

#[ta_objects::QueryParam]
#[derive(Validate)]
pub struct ListParams {
    /// The desired length for the result list (for now, default is 50).
    #[validate(minimum = 0)]
    #[validate(maximum = 1000)]
    #[serde(default = "default_page_len")]
    pub len: usize,
    #[serde(alias = "search", default)]
    pub filter: Vec<String>,
    #[serde(alias = "order-by", default)]
    pub order_by: Option<String>,
    #[serde(default)]
    pub previous: Option<String>,
    #[serde(default)]
    pub next: Option<String>,
    /// The natural ID of the entity used in pagination.
    #[serde(default)]
    pub pagination_id: Option<String>,
}

impl Default for ListParams {
    fn default() -> Self {
        ListParams {
            len: DEFAULT_PAGE_LEN,
            filter: Vec::new(),
            order_by: None,
            previous: None,
            next: None,
            pagination_id: None,
        }
    }
}

/// Request to list entities. Project-free like the other CRUD requests; a
/// tenant request pairs it with its project via the blanket `ScopedExt`
/// trait's `in_project` (see `td_database::pool::scoped`).
#[ta_objects::Dlo]
pub struct ListRequest<N = ()> {
    pub context: RequestContext,
    pub name: N,
    pub list_params: ListParams,
}

impl<N> ListRequest<N> {
    pub fn into_parts(self) -> (RequestContext, N, ListParams) {
        (self.context, self.name, self.list_params)
    }
}

impl RequestContext {
    /// Creates a create request. Scope it to a project with
    /// [`CreateRequest::in_project`].
    pub fn create<C, N>(self, name: impl Into<N>, data: C) -> CreateRequest<C, N> {
        CreateRequest {
            context: self,
            name: name.into(),
            data,
        }
    }

    /// Creates an update request. Scope it to a project with
    /// [`UpdateRequest::in_project`].
    pub fn update<U, N>(self, name: impl Into<N>, data: U) -> UpdateRequest<U, N> {
        UpdateRequest {
            context: self,
            name: name.into(),
            data,
        }
    }

    /// Creates a delete request. Scope it to a project with
    /// [`DeleteRequest::in_project`].
    pub fn delete<N>(self, name: impl Into<N>) -> DeleteRequest<N> {
        DeleteRequest {
            context: self,
            name: name.into(),
        }
    }

    /// Creates a get request. Scope it to a project with
    /// [`ReadRequest::in_project`].
    pub fn read<N>(self, name: impl Into<N>) -> ReadRequest<N> {
        ReadRequest {
            context: self,
            name: name.into(),
        }
    }

    /// Creates a list request. Scope it to a project with
    /// [`ListRequest::in_project`].
    pub fn list<N>(self, name: impl Into<N>, list_params: impl Into<ListParams>) -> ListRequest<N> {
        ListRequest {
            context: self,
            name: name.into(),
            list_params: list_params.into(),
        }
    }
}

#[td_error]
pub enum CrudlErrorX {
    #[error("Cannot create, a unique value in data already exists: {0}")]
    CannotCreateUniqueValueExists(String) = 0,
    #[error("Cannot update, a unique value in data already exists: {0}")]
    CannotUpdateUniqueValueExists(String) = 1,
    #[error("Cannot delete: {0}")]
    CannotDelete(String) = 2,

    #[error("Not found.")]
    NotFound = 1000,

    #[error("Not allowed: {0}")]
    NotAllowed(String) = 2000,

    #[error("Forbidden: {0}")]
    Forbidden(String) = 3000,

    #[error("Invalid credentials")]
    Unauthorized = 4000,
    #[error("Invalid old password")]
    InvalidOldPassword = 4001,

    #[error("Internal error: {0}")]
    InternalError(String) = 5002,
}

#[ta_objects::Dto]
pub struct ListResponse<LL> {
    pub list_params: ListParams,
    pub len: usize,
    pub data: Vec<LL>,

    pub previous: Option<String>,
    pub previous_pagination_id: Option<String>,

    pub next: Option<String>,
    pub next_pagination_id: Option<String>,
}

/// `?at=` — the point in time a historical read resolves at. Shared by every
/// domain with a versioned read surface (functions, tables).
#[ta_objects::QueryParam]
pub struct AtTimeParam {
    #[serde(default)]
    pub at: AtTime,
}
