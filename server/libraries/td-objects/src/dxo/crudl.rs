//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{AccessTokenId, AtTime, RoleId, UserId};
use serde::{Deserialize, Serialize};
use serde_valid::Validate;
use sqlx::Error;
use sqlx::error::ErrorKind::{ForeignKeyViolation, UniqueViolation};
use sqlx::sqlite::SqliteQueryResult;
use std::fmt::Debug;
use td_database::sql::DbError;
use td_error::{TdDomainError, TdError, td_error};
use td_tower::error::{ConnectionError, FromHandlerError};

/// Request context for the logic layer.
#[td_type::Dlo]
pub struct RequestContext {
    /// The ID of the access token in the request.
    #[td_type(extractor)]
    pub access_token_id: AccessTokenId,
    /// The ID of the user making the request.
    #[td_type(extractor)]
    pub user_id: UserId,
    /// The role of the user making the request.
    #[td_type(extractor)]
    pub role_id: RoleId,
    /// The time the request was made.
    #[td_type(extractor)]
    pub time: AtTime,
}

impl RequestContext {
    pub fn with(
        access_token_id: impl Into<AccessTokenId>,
        user_id: impl Into<UserId>,
        role_id: impl Into<RoleId>,
    ) -> Self {
        Self {
            access_token_id: access_token_id.into(),
            user_id: user_id.into(),
            role_id: role_id.into(),
            time: AtTime::default(),
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
    pub context: RequestContext,
    #[td_type(extractor)]
    pub name: Name<N>,
    /// The data to create the entity.
    #[td_type(extractor)]
    pub data: Data<C>,
}

/// Request to update an entity.
#[td_type::Dlo]
pub struct UpdateRequest<N: Clone, U: Clone> {
    #[td_type(extractor)]
    pub context: RequestContext,
    /// The logical name of the entity to update.
    #[td_type(extractor)]
    pub name: Name<N>,
    /// The data to update the entity.
    #[td_type(extractor)]
    pub data: Data<U>,
}

/// Request to delete an entity.
#[td_type::Dlo]
pub struct DeleteRequest<N: Clone> {
    #[td_type(extractor)]
    pub context: RequestContext,
    /// The logical name of the entity to delete.
    #[td_type(extractor)]
    pub name: Name<N>,
}

/// Request to get an entity.
#[td_type::Dlo]
pub struct ReadRequest<N: Clone> {
    #[td_type(extractor)]
    pub context: RequestContext,
    /// The logical name of the entity to read.
    #[td_type(extractor)]
    pub name: Name<N>,
}

const DEFAULT_PAGE_LEN: usize = 50;

fn default_page_len() -> usize {
    DEFAULT_PAGE_LEN
}

/// List parameters for list operations defining filtering, sorting and pagination.
#[td_type::QueryParam]
#[derive(Validate)]
pub struct ListParams {
    /// The desired length for the result list (for now, default is 10000).
    #[validate(minimum = 0)]
    #[builder(default = "default_page_len()")]
    #[serde(default = "default_page_len")]
    pub len: usize,
    /// The filter to apply when creating the result list.
    #[builder(default)]
    #[serde(alias = "search", default)]
    pub filter: Vec<String>,
    /// The sort order of the result list.
    #[builder(default)]
    #[serde(alias = "order-by", default)]
    pub order_by: Option<String>,
    /// The previous value for pagination.
    #[builder(default)]
    #[serde(default)]
    pub previous: Option<String>,
    /// The next value for pagination.
    #[builder(default)]
    #[serde(default)]
    pub next: Option<String>,
    /// The natural ID of the entity used in pagination.
    #[builder(default)]
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

/// Request to list entities.
#[td_type::Dlo]
pub struct ListRequest<N: Clone> {
    #[td_type(extractor)]
    pub context: RequestContext,
    #[td_type(extractor)]
    pub name: Name<N>,
    pub list_params: ListParams,
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
#[td_type::Dto]
pub struct ListResponse<LL: Clone> {
    /// The list parameters of the request.
    pub list_params: ListParams,
    /// The length of the result list.
    pub len: usize,
    #[builder(setter(custom))]
    /// The data of the result list.
    pub data: Vec<LL>,

    // Pagination info to go to previous page

    //#[builder(private)] NOTE: we cannot do set this because list_status! macro generates a
    //                          concrete class and tries to define the builder with a pub setter.
    //                          As we don't use the ListParam builder in the app code (use by the
    //                          framework only) this is not an issue.
    pub previous: Option<String>,
    //#[builder(private)] NOTE: same same
    pub previous_pagination_id: Option<String>,

    // Pagination info to go to next page

    //#[builder(private)] NOTE: same same
    pub next: Option<String>,
    //#[builder(private)] NOTE: same same
    pub next_pagination_id: Option<String>,
}

impl<LL: Clone> ListResponseBuilder<LL> {
    /// Sets the data for the list response, the length is inferred from the data.
    pub fn data(&mut self, data: Vec<LL>) -> &mut Self {
        self.len = Some(data.len());
        self.data = Some(data);
        self
    }

    /// Sets info to paginate to previous page
    pub fn previous_page(
        &mut self,
        previous: Option<String>,
        previous_pagination_id: Option<String>,
    ) -> &mut Self {
        self.previous = Some(previous);
        self.previous_pagination_id = Some(previous_pagination_id);
        self
    }

    /// Sets info to paginate to next page
    pub fn next_page(
        &mut self,
        next: Option<String>,
        next_pagination_id: Option<String>,
    ) -> &mut Self {
        self.next = Some(next);
        self.next_pagination_id = Some(next_pagination_id);
        self
    }
}

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
