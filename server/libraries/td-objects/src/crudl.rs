//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{AccessTokenId, AtTime, RoleId, UserId};
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use serde_valid::Validate;
use sqlx::error::ErrorKind::{ForeignKeyViolation, UniqueViolation};
use sqlx::sqlite::SqliteQueryResult;
use sqlx::Error;
use std::fmt::Debug;
use td_apiforge::apiserver_schema;
use td_database::sql::DbError;
use td_error::td_error;
use td_error::{TdDomainError, TdError};
use td_tower::error::{ConnectionError, FromHandlerError};
use utoipa::IntoParams;

#[td_type::typed(bool)]
pub struct SysAdmin;

/// Request context for the logic layer.
#[td_type::Dlo]
pub struct RequestContext {
    /// The ID of the access token in the request.
    #[td_type(extractor)]
    access_token_id: AccessTokenId,
    /// The ID of the user making the request.
    #[td_type(extractor)]
    user_id: UserId,
    /// The role of the user making the request.
    #[td_type(extractor)]
    role_id: RoleId,
    /// if the role has system admin privileges.
    #[td_type(extractor)]
    sys_admin: SysAdmin,
    /// The time the request was made.
    #[td_type(extractor)]
    time: AtTime,
}

impl RequestContext {
    //TODO: change signature to remove sys_admin when permissions are fully integrated
    pub fn with(
        access_token_id: impl Into<AccessTokenId>,
        user_id: impl Into<UserId>,
        role_id: impl Into<RoleId>,
        sys_admin: impl Into<SysAdmin>,
    ) -> Self {
        Self {
            access_token_id: access_token_id.into(),
            user_id: user_id.into(),
            role_id: role_id.into(),
            sys_admin: sys_admin.into(),
            time: AtTime::default(),
        }
    }

    pub fn assert_sys_admin(&self) -> Result<(), TdError> {
        if *self.sys_admin {
            Ok(())
        } else {
            Err(CrudlErrorX::Forbidden(String::from(
                "Current role does not have sysadmin permission",
            )))?
        }
    }
}

pub trait IntoName<T> {
    fn into_name(self) -> T;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Name<N>(N);

impl<N> IntoName<N> for Name<N> {
    fn into_name(self) -> N {
        self.0
    }
}

pub trait IntoData<D> {
    fn into_data(self) -> D;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Data<D>(D);

impl<D> IntoData<D> for Data<D> {
    fn into_data(self) -> D {
        self.0
    }
}

/// Request to create an entity.
#[td_type::Dlo]
pub struct CreateRequest<N: Clone, C: Clone> {
    #[td_type(extractor)]
    context: RequestContext,
    #[td_type(extractor)]
    name: Name<N>,
    /// The data to create the entity.
    #[td_type(extractor)]
    data: Data<C>,
}

/// Request to update an entity.
#[td_type::Dlo]
pub struct UpdateRequest<N: Clone, U: Clone> {
    #[td_type(extractor)]
    context: RequestContext,
    /// The logical name of the entity to update.
    #[td_type(extractor)]
    name: Name<N>,
    /// The data to update the entity.
    #[td_type(extractor)]
    data: Data<U>,
}

/// Request to delete an entity.
#[td_type::Dlo]
pub struct DeleteRequest<N: Clone> {
    #[td_type(extractor)]
    context: RequestContext,
    /// The logical name of the entity to delete.
    #[td_type(extractor)]
    name: Name<N>,
}

/// Request to get an entity.
#[td_type::Dlo]
pub struct ReadRequest<N: Clone> {
    #[td_type(extractor)]
    context: RequestContext,
    /// The logical name of the entity to read.
    #[td_type(extractor)]
    name: Name<N>,
}

/// List parameters for list operations defining filtering, sorting and pagination.
#[apiserver_schema]
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, Getters, IntoParams, Builder,
)]
#[builder(setter(into), default)]
#[getset(get = "pub")]
pub struct ListParams {
    /// The desired offset for the result list.
    #[validate(minimum = 0)]
    #[serde(default = "ListParams::default_offset")]
    offset: usize,
    /// The desired length for the result list (for now, default is 10000).
    #[validate(minimum = 0)]
    #[serde(default = "ListParams::default_len")]
    len: usize,
    /// The filter to apply when creating the result list.
    #[serde(alias = "search", default)]
    filter: Vec<String>,
    /// The sort order of the result list.
    #[serde(alias = "order-by", default)]
    order_by: Option<String>,
    /// The previous value for pagination.
    #[serde(default)]
    previous: Option<String>,
    /// The next value for pagination.
    #[serde(default)]
    next: Option<String>,
    /// The natural ID of the entity used in pagination.
    #[serde(default)]
    natural_id: Option<String>,
}

impl ListParams {
    const DEFAULT_OFFSET: usize = 0;
    const DEFAULT_LEN: usize = 10000;

    fn default_offset() -> usize {
        Self::DEFAULT_OFFSET
    }

    fn default_len() -> usize {
        Self::DEFAULT_LEN
    }

    pub fn first() -> Self {
        Self {
            offset: Self::default_offset(),
            len: 1,
            filter: Vec::new(),
            order_by: None,
            previous: None,
            next: None,
            natural_id: None,
        }
    }

    pub fn all() -> Self {
        ListParams {
            offset: 0,
            len: usize::MAX - 1,
            filter: Vec::new(),
            order_by: None,
            previous: None,
            next: None,
            natural_id: None,
        }
    }
}

impl Default for ListParams {
    fn default() -> Self {
        ListParams {
            offset: Self::default_offset(),
            len: Self::default_len(),
            filter: Vec::new(),
            order_by: None,
            previous: None,
            next: None,
            natural_id: None,
        }
    }
}

/// Request to list entities.
#[td_type::Dlo]
pub struct ListRequest<N: Clone> {
    #[td_type(extractor)]
    context: RequestContext,
    #[td_type(extractor)]
    name: Name<N>,
    list_params: ListParams,
}

/// Convenience API enum for fields that need to add or remove items.
///
/// For example, when updating a role, to add or remove users from the role.
#[derive(Debug, Clone, Deserialize)]
pub enum AddOrRemove<T: Debug + Clone> {
    /// Add the given items.
    Add(Vec<T>),
    /// Remove the given items.
    Remove(Vec<T>),
    /// Remove all items.
    RemoveAll,
}

impl RequestContext {
    /// Creates a create request.
    pub fn create<N: Clone, C: Clone>(self, name: impl Into<N>, data: C) -> CreateRequest<N, C> {
        CreateRequest {
            context: self,
            name: Name(name.into()),
            data: Data(data),
        }
    }

    /// Creates an update request.
    pub fn update<N: Clone, U: Clone>(self, name: impl Into<N>, data: U) -> UpdateRequest<N, U> {
        UpdateRequest {
            context: self,
            name: Name(name.into()),
            data: Data(data),
        }
    }

    /// Creates a delete request.
    pub fn delete<N: Clone>(self, name: impl Into<N>) -> DeleteRequest<N> {
        DeleteRequest {
            context: self,
            name: Name(name.into()),
        }
    }

    /// Creates a get request.
    pub fn read<N: Clone>(self, name: impl Into<N>) -> ReadRequest<N> {
        ReadRequest {
            context: self,
            name: Name(name.into()),
        }
    }

    /// Creates a list request.
    pub fn list<N: Clone>(
        self,
        name: impl Into<N>,
        list_params: impl Into<ListParams>,
    ) -> ListRequest<N> {
        ListRequest {
            context: self,
            name: Name(name.into()),
            list_params: list_params.into(),
        }
    }
}

/// Error returned by the logic layer operations.
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

    #[error("Connection Error: {0}")]
    ConnectionError(#[source] ConnectionError) = 5000,
    #[error("Service Handler Error: {0}")]
    ServiceHandlerError(#[from] FromHandlerError) = 5001,
    #[error("Internal error: {0}")]
    InternalError(String) = 5002,
}

/// Response for list operations.
///
/// Besides the data, it includes the [`ListParams`] used for the list operation,
/// the offset and length of the result and a flag indicating if there are more results or not.
#[derive(Debug, Clone, Serialize, Deserialize, Getters, Builder)]
#[builder(pattern = "owned")]
#[getset(get = "pub")]
pub struct ListResponse<LL> {
    // TODO: TD-259, see if necessary to handle offset differently for possible gaps due to
    // TODO: data being changed while paginating
    /// The list parameters of the request.
    list_params: ListParams,
    #[builder(default = "0")]
    /// The offset of the result list.
    offset: usize,
    /// The length of the result list.
    len: usize,
    #[builder(setter(custom))]
    /// The data of the result list.
    data: Vec<LL>,
    /// Flag indicating if there are more results.
    more: bool,
}

impl<LL> ListResponseBuilder<LL> {
    /// Sets the data for the list response, the length is inferred from the data.
    pub fn data(mut self, data: Vec<LL>) -> Self {
        self.len = Some(data.len());
        self.data = Some(data);
        self
    }
}

/// Helper function that creates a [`ListResponse`].
pub fn list_response<LL>(list_params: &ListParams, list: Vec<LL>) -> ListResponse<LL> {
    let mut list = list;
    let more = list.len() > *list_params.len();
    more.then(|| list.pop());
    // if more {
    //     list.pop();
    // }
    let offset = *list_params.offset();
    ListResponseBuilder::default()
        .data(list)
        .list_params(list_params.clone())
        .offset(offset)
        .more(more)
        .build()
        .unwrap()
}

// TODO [TD-258] fine tune SQL error handling [TD-239] to correctly identify dup keys issues, dup keys issues, etc.

/// Crudl helper function to handle SQL create errors.
pub fn handle_create_error(e: Error) -> CrudlErrorX {
    match e {
        Error::Database(db_err) => {
            let msg = db_err.message().to_string();
            match db_err.kind() {
                UniqueViolation => CrudlErrorX::CannotCreateUniqueValueExists(msg),
                _ => CrudlErrorX::InternalError(msg),
            }
        }
        _ => CrudlErrorX::InternalError(e.to_string()),
    }
}

/// Crudl helper function to handle SQL update errors.
pub fn handle_update_error(e: Error) -> CrudlErrorX {
    match e {
        Error::Database(db_err) => {
            let msg = db_err.message().to_string();
            match db_err.kind() {
                UniqueViolation => CrudlErrorX::CannotUpdateUniqueValueExists(msg),
                _ => CrudlErrorX::InternalError(msg),
            }
        }
        _ => CrudlErrorX::InternalError(e.to_string()),
    }
}

/// Crudl helper function to handle SQL get errors.
pub fn handle_select_error(e: Error) -> CrudlErrorX {
    match e {
        Error::RowNotFound => CrudlErrorX::NotFound,
        _ => CrudlErrorX::InternalError(e.to_string()),
    }
}

/// Converts a SQL error into an app error.
pub fn handle_sql_err(err: Error) -> TdError {
    TdError::new(DbError::SqlError(err))
}

pub fn handle_create_unique_err<AlreadyExisting, DbErr>(
    already_existing: AlreadyExisting,
    from_db_err: fn(Error) -> DbErr,
) -> impl FnOnce(Error) -> TdError
where
    AlreadyExisting: TdDomainError + 'static,
    DbErr: TdDomainError + 'static,
{
    move |err| match err {
        Error::Database(err) if err.kind() == UniqueViolation => TdError::new(already_existing),
        _ => TdError::new(from_db_err(err)),
    }
}

/// Converts a SQL error into an app error with special handling of a NOT_FOUND error.
///
/// To be used with SQL [`sqlx::query::QueryAs::fetch_one`] calls.
pub fn handle_select_one_err<NotFound, DbErr>(
    not_found: NotFound,
    from_db_err: fn(Error) -> DbErr,
) -> impl FnOnce(Error) -> TdError
where
    NotFound: TdDomainError + 'static,
    DbErr: TdDomainError + 'static,
{
    move |err| match err {
        Error::RowNotFound => TdError::new(not_found),
        _ => TdError::new(from_db_err(err)),
    }
}

/// Crudl helper function to handle SQL delete errors.
pub fn handle_delete_error(e: Error) -> CrudlErrorX {
    match e {
        Error::Database(db_err) => {
            let msg = db_err.message().to_string();
            match db_err.kind() {
                ForeignKeyViolation => CrudlErrorX::CannotDelete(msg),
                _ => CrudlErrorX::InternalError(msg),
            }
        }
        _ => CrudlErrorX::InternalError(e.to_string()),
    }
}

/// Crudl helper function to handle SQL list errors.
pub fn handle_list_error(e: Error) -> CrudlErrorX {
    CrudlErrorX::InternalError(e.to_string())
}

/// Helper function to assert that only one row was affected by the SQL operation.
pub fn assert_one(res: SqliteQueryResult) -> Result<(), CrudlErrorX> {
    if res.rows_affected() == 0 {
        return Err(CrudlErrorX::NotFound);
    }
    Ok(())
}
