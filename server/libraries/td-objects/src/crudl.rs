//
// Copyright 2025 Tabs Data Inc.
//

use crate::entity_finder::EntityFinderError;
use crate::types::basic::RoleId;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use serde_valid::Validate;
use sqlx::error::ErrorKind::{ForeignKeyViolation, UniqueViolation};
use sqlx::sqlite::{SqliteQueryResult, SqliteRow};
use sqlx::{Error, SqliteConnection};
use std::fmt::Debug;
use td_apiforge::apiserver_schema;
use td_common::time::UniqueUtc;
use td_database::sql::DbError;
use td_error::td_error;
use td_error::{TdDomainError, TdError};
use td_tower::error::{ConnectionError, FromHandlerError};
use utoipa::IntoParams;

/// Request context for the logic layer.
#[derive(Clone, Debug, Getters)]
#[getset(get = "pub")]
pub struct RequestContext {
    /// The ID of the user making the request.
    //TODO: change to UserId
    user_id: String,
    /// The role of the user making the request.
    role_id: RoleId,
    /// if the role has system admin privileges.
    sys_admin: bool,
    /// The time the request was made.
    //TODO: change to AtTime
    time: DateTime<Utc>,
}

impl RequestContext {
    //TODO: change signature to
    //    with(user_id: impl Into<UserId>, role_id: impl Into<RoleId>) -> Self
    pub async fn with(user_id: impl Into<String>, role_id: &str, sys_admin: bool) -> Self {
        //TODO:   TEMP until we change signature
        let role_id = role_id.try_into().unwrap_or(RoleId::default());
        Self {
            user_id: user_id.into(),
            role_id,
            sys_admin,
            time: UniqueUtc::now_millis().await,
        }
    }

    pub fn assert_sys_admin(&self) -> Result<(), TdError> {
        if self.sys_admin {
            Ok(())
        } else {
            Err(CrudlErrorX::Forbidden(String::from(
                "Current role does not have sysadmin permission",
            )))?
        }
    }
}

/// Request to create an entity.
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct CreateRequest<N, C> {
    context: RequestContext,
    name: Name<N>,
    /// The data to create the entity.
    data: C,
}

// The logical name of the entity.
#[derive(Debug, Clone)]
pub struct Name<N = String> {
    value: N,
}

impl<N> Name<N> {
    pub fn new(value: impl Into<N>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn value(&self) -> &N {
        &self.value
    }
}

impl<N> From<N> for Name<N> {
    fn from(value: N) -> Self {
        Self::new(value)
    }
}

/// Request to update an entity.
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct UpdateRequest<N, U> {
    context: RequestContext,
    /// The logical name of the entity to update.
    name: Name<N>,
    /// The data to update the entity.
    data: U,
}

/// Request to delete an entity.
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct DeleteRequest<N> {
    context: RequestContext,
    /// The logical name of the entity to delete.
    name: Name<N>,
}

/// Request to get an entity.
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct ReadRequest<N> {
    context: RequestContext,
    /// The logical name of the entity to read.
    name: Name<N>,
}

/// List parameters for list operations defining filtering, sorting and pagination.
#[apiserver_schema]
#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, Validate, Getters, IntoParams, Builder,
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
    /// The filter to apply when creating the result list (not yet implemented).
    #[serde(alias = "search", default)]
    filter: String, // TODO define filter syntax (consider RSQL) [TD-240]
    /// The sort order of the result list (not yet implemented).
    #[serde(alias = "order-by", default)]
    order_by: String, // TODO define sort syntax [TD-240]
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
            filter: String::new(),
            order_by: String::new(),
        }
    }
}

impl Default for ListParams {
    fn default() -> Self {
        ListParams {
            offset: Self::default_offset(),
            len: Self::default_len(),
            filter: String::new(),
            order_by: String::new(),
        }
    }
}

/// Request to list entities.
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct ListRequest<N> {
    context: RequestContext,
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
    pub fn create<N, C>(self, name: impl Into<N>, data: C) -> CreateRequest<N, C> {
        CreateRequest {
            context: self,
            name: Name::new(name),
            data,
        }
    }

    /// Creates an update request.
    pub fn update<N, U>(self, name: impl Into<N>, data: U) -> UpdateRequest<N, U> {
        UpdateRequest {
            context: self,
            name: Name::new(name),
            data,
        }
    }

    /// Creates a delete request.
    pub fn delete<N>(self, name: impl Into<N>) -> DeleteRequest<N> {
        DeleteRequest {
            context: self,
            name: Name::new(name),
        }
    }

    /// Creates a get request.
    pub fn read<N>(self, name: impl Into<N>) -> ReadRequest<N> {
        ReadRequest {
            context: self,
            name: Name::new(name),
        }
    }

    /// Creates a list request.
    pub fn list<N>(self, name: impl Into<N>, list_params: impl Into<ListParams>) -> ListRequest<N> {
        ListRequest {
            context: self,
            name: Name::new(name),
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
    #[error("Bad request: {0}")]
    BadRequest(String) = 3,
    #[error("Invalid list parameter '{0}': {1}")]
    InvalidListParams(String, String) = 4,
    #[error("Dependency not found: {0}")]
    DependencyNotFound(#[from] EntityFinderError) = 5,
    #[error("Invalid trigger URI: {0}, {1}")]
    InvalidTriggerUri(String, String) = 6,
    #[error("Invalid dependency URI: {0}, {1}")]
    InvalidDependencyUri(String, String) = 7,

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

impl<LL> ListResponse<LL> {
    /// Creates a builder for a list response.
    pub fn builder() -> ListResponseBuilder<LL> {
        ListResponseBuilder::default()
    }
}

/// Convenience empty CRULD context type for when no context is not needed.
#[derive(Debug, Default)]
pub struct NoContext {}

/// Helper function to create a SQL select for list (paginated) results. It should be used
/// in tandem with [`list_result`].
///
/// It adds LIMIT and OFFSET to the given SQL extracting the length and offset from the given
/// [`ListParams`].
///
/// It adds one to the length to determine if there are more data in the database.
///
/// The query result must be passed through the [`list_result`] function to determine if
/// there is more data in the database and to remove the extra row if necessary.
pub fn list_select(list_params: &ListParams, sql: &str) -> String {
    format!(
        "{} LIMIT {} OFFSET {}",
        sql,
        list_params.len() + 1,
        list_params.offset()
    )
}

#[derive(Debug, Serialize, Getters)]
#[getset(get = "pub")]
pub struct ListResult<T> {
    pub list: Vec<T>,
    pub more: bool,
}

impl<T> ListResult<T> {
    pub fn new(list: Vec<T>, more: bool) -> Self {
        Self { list, more }
    }

    pub fn map<B, F>(&self, mapper: F) -> ListResult<B>
    where
        F: FnMut(&T) -> B,
    {
        ListResult {
            list: self.list().iter().map(mapper).collect(),
            more: self.more,
        }
    }

    pub fn try_map<B, F, E>(&self, mapper: F) -> Result<ListResult<B>, E>
    where
        F: FnMut(&T) -> Result<B, E>,
    {
        let list: Vec<B> = self
            .list
            .iter()
            .map(mapper)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ListResult {
            list,
            more: self.more,
        })
    }
}

/// Helper function to determine if there is more data in the database. It should be used
/// in tandem with [`list_select`].
///
/// Determines if there is more data in the database and removes the extra row if necessary.
pub fn list_result<LL>(list_params: ListParams, list: Vec<LL>) -> ListResult<LL> {
    let mut list = list;
    let more = list.len() > *list_params.len();
    more.then(|| list.pop());
    // if more {
    //     list.pop();
    // }
    ListResult { list, more }
}

impl<LL> From<ListResult<LL>> for (Vec<LL>, bool) {
    fn from(val: ListResult<LL>) -> Self {
        (val.list, val.more)
    }
}

/// Helper function that creates a [`ListResponse`].
pub fn list_response<LL>(list_params: ListParams, list_result: ListResult<LL>) -> ListResponse<LL> {
    let offset = *list_params.offset();
    ListResponseBuilder::default()
        .data(list_result.list)
        .list_params(list_params)
        .offset(offset)
        .more(list_result.more)
        .build()
        .unwrap()
}

/// Helper function to select a single row from the database by a key.
pub async fn select_by<DB>(
    conn: &mut SqliteConnection,
    select_by_sql: &str,
    key: &str,
) -> Result<DB, CrudlErrorX>
where
    DB: Unpin + Send + for<'r> sqlx::FromRow<'r, SqliteRow>,
{
    sqlx::query_as(select_by_sql)
        .bind(key.to_string())
        .fetch_one(conn)
        .await
        .map_err(handle_select_error)
}

/// Helper function to select multiple row from the database by a key.
pub async fn select_all_by<DB>(
    conn: &mut SqliteConnection,
    select_by_sql: &str,
    key: &str,
) -> Result<Vec<DB>, CrudlErrorX>
where
    DB: Unpin + Send + for<'r> sqlx::FromRow<'r, SqliteRow>,
{
    sqlx::query_as(select_by_sql)
        .bind(key.to_string())
        .fetch_all(conn)
        .await
        .map_err(handle_select_error)
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
