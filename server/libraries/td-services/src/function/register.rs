//
// Copyright 2025 Tabsdata Inc.
//

use td_bridge::{BridgeProviderHandle, Orchestrator};
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::DisplayVec;
use td_error::{TdError, td_error};
use td_objects::dxo::collection::ActiveCollectionDB;
use td_objects::dxo::connection::ConnectionDB;
use td_objects::dxo::function::FunctionInsertDB;
use td_objects::types::basic::{
    CollectionName, CollectionType, ConnectionKind, Decorator, FunctionName, TableName,
    TableNameDto, TableType,
};
use td_objects::types::composed::{TableDependency, TableTrigger};
use td_queries::Queries;

#[td_error]
pub enum RegisterFunctionError {
    #[error("Tables [{0}] already exists in collection '{1}'")]
    TableAlreadyExists(DisplayVec<TableName>, CollectionName) = 0,
    #[error("Dependency table '{0}' does not exist")]
    DependencyTableDoesNotExist(TableDependency) = 1,
    #[error("Table '{0}' cannot trigger its own function")]
    SelfTrigger(TableTrigger) = 2,
    #[error("Trigger table '{0}' does not exist")]
    TriggerTableDoesNotExist(TableTrigger) = 3,
    #[error(
        "Table '{0}' exists in collection '{1}' but is frozen; set reuse_frozen_tables to adopt it"
    )]
    FrozenTableAlreadyExists(TableName, CollectionName) = 4,
    #[error("Duplicate output tables defined: [{0}]")]
    DuplicateTableDefinition(DisplayVec<TableName>) = 6,
    #[error("A {0:?} function must declare the connection kind it uses")]
    ConnectionKindRequired(Decorator) = 7,
    #[error("A transformer function cannot declare a connection kind (got '{0}')")]
    ConnectionKindNotAllowed(ConnectionKind) = 8,
    #[error("Collection '{0}' has no connection; a publisher/subscriber requires one")]
    CollectionHasNoConnection(CollectionName) = 9,
    #[error("Connection kind '{0}' does not match collection '{2}' connection kind '{1}'")]
    ConnectionKindMismatch(ConnectionKind, ConnectionKind, CollectionName) = 10,
    #[error("A transformer function cannot be registered in collection '{0}', a {1:?} collection")]
    TransformerInConnectedCollection(CollectionName, CollectionType) = 11,
    #[error("Parent table '{0}' does not exist in collection '{1}'")]
    ParentTableDoesNotExist(TableName, CollectionName) = 19,
    #[error("Table '{0}' is not produced by function '{1}', so no table can derive from it")]
    ParentTableNotOwned(TableNameDto, FunctionName) = 20,
    #[error("Table '{0}' is a {1:?} table, so it cannot be the parent of another table")]
    ParentTableMustBeRegular(TableName, TableType) = 21,
    #[error("Table '{0}' is a Regular table, so it cannot be derived from a parent table")]
    RegularTableCannotHaveParent(TableName) = 22,
}

/// A publisher/subscriber must declare the connection kind it uses, its
/// collection must carry a connection, and the two kinds must match. A
/// transformer must declare none, and must be in a `DataFactory` collection
/// unless it is a child function, which lives wherever its parent does.
///
/// TODO FOR 2.0: this contract is enforced only under the Kubernetes
/// orchestrator (the 2.x deployment). The 1.x local Supervisor still runs
/// the legacy `tabsdata` client, which declares no connection kind and uses
/// connection-less collections, so the contract cannot hold there -- the
/// guard below skips the check. Remove the guard once the legacy client is
/// retired, so the check applies under every orchestrator.
pub async fn ensure_connection_kind_matches<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    bridge: &BridgeProviderHandle,
    function: &FunctionInsertDB,
    collection: &ActiveCollectionDB,
) -> Result<(), TdError> {
    // TODO FOR 2.0: drop this guard (see fn doc) -- enforce on every orchestrator.
    if !matches!(bridge.orchestrator(), Orchestrator::Kubernetes) {
        return Ok(());
    }
    match &function.decorator {
        Decorator::Publisher | Decorator::Subscriber => {
            let function_connection = function.connection.as_ref().ok_or_else(|| {
                RegisterFunctionError::ConnectionKindRequired(function.decorator.clone())
            })?;
            let connection_id = collection.connection_id.ok_or_else(|| {
                RegisterFunctionError::CollectionHasNoConnection(collection.name.clone())
            })?;
            let connection: ConnectionDB = conn.fetch_one(connection_id).await?;
            if *function_connection != connection.base.kind {
                Err(RegisterFunctionError::ConnectionKindMismatch(
                    function_connection.clone(),
                    connection.base.kind.clone(),
                    collection.name.clone(),
                ))?
            }
        }
        Decorator::Transformer => {
            if let Some(connection) = function.connection.as_ref() {
                Err(RegisterFunctionError::ConnectionKindNotAllowed(
                    connection.clone(),
                ))?
            }

            if function.parent_function_id.is_none()
                && !matches!(collection.collection_type, CollectionType::DataFactory)
            {
                Err(RegisterFunctionError::TransformerInConnectedCollection(
                    collection.name.clone(),
                    collection.collection_type.clone(),
                ))?
            }
        }
    }
    Ok(())
}
