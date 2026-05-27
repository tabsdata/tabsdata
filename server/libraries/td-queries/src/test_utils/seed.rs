//
// Copyright 2026 Tabs Data Inc.
//

//! Test-only seed surface. Reach via [`SeedEntry::seed`] on any
//! [`QueryExecutor`] (e.g. a `TdPool` in test code), then call the
//! per-DAO seed method:
//!
//! ```ignore
//! let collection = db.seed().collection(&name, &user).await?;
//! let function = db.seed().function(&collection, &create).await?;
//! ```
//!
//! Only the `test-utils` feature exposes this surface — production code
//! cannot reach it.

use crate::QueryExecutor;
use crate::test_utils::seed_collection::{seed_collection, seed_collection_in_group};
use crate::test_utils::seed_execution::{seed_cron_execution, seed_execution};
use crate::test_utils::seed_function::seed_function;
use crate::test_utils::seed_group::seed_group;
use crate::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
use crate::test_utils::seed_logical_input::seed_logical_input;
use crate::test_utils::seed_permission::seed_permission;
use crate::test_utils::seed_physical_function_run::seed_physical_function_run;
use crate::test_utils::seed_physical_table_data_version::{
    seed_physical_table_data_version, seed_physical_table_data_version_with_data,
};
use crate::test_utils::seed_role::seed_role;
use crate::test_utils::seed_system_tables::seed_system_tables;
use crate::test_utils::seed_transaction::seed_transaction;
use crate::test_utils::seed_user::seed_user;
use crate::test_utils::seed_user_role::seed_user_role;
use crate::test_utils::seed_worker::{seed_function_runner_worker, seed_worker};
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::inter_collection_permission::InterCollectionPermissionDB;
use td_objects::dxo::logical::LogicalFunctionRunInputDB;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    CollectionId, CollectionName, DependencyPos, Description, EntityId, EntityName, GroupId,
    GroupName, LogicalFunctionRunStatus, PermissionType, RoleId, RoleName, ToCollectionId,
    TransactionKey, UserEnabled, UserId, UserName, VersionPos, WorkerMessageStatus, WorkerStatus,
    WorkerType,
};

query_handle! {
    /// Entry trait providing [`seed`](SeedEntry::seed) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Seed`] handle.
    pub trait SeedEntry {
        fn seed(self) -> Seed<Self> {
            Seed { executor: self }
        }
    }
}

pub struct Seed<E> {
    pub(crate) executor: E,
}

impl<E> Seed<E>
where
    E: QueryExecutor + Copy,
{
    pub async fn collection(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
    ) -> Result<CollectionDB, TdError> {
        seed_collection(self.executor, collection_name, created_by).await
    }

    pub async fn collection_in_group(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
        group_id: GroupId,
    ) -> Result<CollectionDB, TdError> {
        seed_collection_in_group(self.executor, collection_name, created_by, group_id).await
    }

    pub async fn execution(self, function_version: &FunctionDB) -> Result<ExecutionDB, TdError> {
        seed_execution(self.executor, function_version).await
    }

    pub async fn cron_execution(
        self,
        function_version: &FunctionDB,
    ) -> Result<ExecutionDB, TdError> {
        seed_cron_execution(self.executor, function_version).await
    }

    pub async fn function(
        self,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
    ) -> Result<FunctionDB, TdError> {
        seed_function(self.executor, collection, function_create).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn logical_input(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
        requirement_table: &TableDB,
        requirement_function_run: Option<&PhysicalFunctionRunDB>,
        requirement_table_data_version: Option<&PhysicalTableDataVersionDB>,
        requirement_dependency_pos: Option<&DependencyPos>,
        requirement_version_pos: &VersionPos,
    ) -> Result<LogicalFunctionRunInputDB, TdError> {
        seed_logical_input(
            self.executor,
            collection,
            execution,
            transaction,
            physical_function_run,
            requirement_table,
            requirement_function_run,
            requirement_table_data_version,
            requirement_dependency_pos,
            requirement_version_pos,
        )
        .await
    }

    pub async fn physical_function_run(
        self,
        collection: &CollectionDB,
        function_version: &FunctionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        status: &LogicalFunctionRunStatus,
    ) -> Result<PhysicalFunctionRunDB, TdError> {
        seed_physical_function_run(
            self.executor,
            collection,
            function_version,
            execution,
            transaction,
            status,
        )
        .await
    }

    pub async fn inter_collection_permission(
        self,
        from_collection: &CollectionId,
        to_collection: &ToCollectionId,
    ) -> Result<InterCollectionPermissionDB, TdError> {
        seed_inter_collection_permission(self.executor, from_collection, to_collection).await
    }

    pub async fn permission(
        self,
        permission_type: PermissionType,
        entity_name: Option<EntityName>,
        entity_id: Option<EntityId>,
        role_db: &RoleDB,
    ) -> Result<PermissionDB, TdError> {
        seed_permission(
            self.executor,
            permission_type,
            entity_name,
            entity_id,
            role_db,
        )
        .await
    }

    pub async fn role(self, name: RoleName, description: Description) -> Result<RoleDB, TdError> {
        seed_role(self.executor, name, description).await
    }

    pub async fn group(
        self,
        name: GroupName,
        description: Description,
    ) -> Result<GroupDB, TdError> {
        seed_group(self.executor, name, description).await
    }

    pub async fn system_tables(self, function: &FunctionDB) -> Result<Vec<TableDB>, TdError> {
        seed_system_tables(self.executor, function).await
    }

    pub async fn physical_table_data_version(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
        table: &TableDB,
    ) -> Result<PhysicalTableDataVersionDB, TdError> {
        seed_physical_table_data_version(
            self.executor,
            collection,
            execution,
            transaction,
            physical_function_run,
            table,
        )
        .await
    }

    pub async fn physical_table_data_version_with_data(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
        table: &TableDB,
    ) -> Result<PhysicalTableDataVersionDB, TdError> {
        seed_physical_table_data_version_with_data(
            self.executor,
            collection,
            execution,
            transaction,
            physical_function_run,
            table,
        )
        .await
    }

    pub async fn transaction(
        self,
        execution: &ExecutionDB,
        transaction_key: &TransactionKey,
    ) -> Result<TransactionDB, TdError> {
        seed_transaction(self.executor, execution, transaction_key).await
    }

    pub async fn user(self, name: &UserName, enabled: &UserEnabled) -> Result<UserDB, TdError> {
        seed_user(self.executor, name, enabled).await
    }

    pub async fn user_role(self, user: &UserId, role: &RoleId) -> Result<UserRoleDB, TdError> {
        seed_user_role(self.executor, user, role).await
    }

    pub async fn worker(
        self,
        collection: &CollectionDB,
        function: &FunctionDB,
        worker_type: &WorkerType,
        worker_status: &WorkerStatus,
        message_status: &WorkerMessageStatus,
    ) -> Result<WorkerDB, TdError> {
        seed_worker(
            self.executor,
            collection,
            function,
            worker_type,
            worker_status,
            message_status,
        )
        .await
    }

    pub async fn function_runner_worker(
        self,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
        status: &WorkerMessageStatus,
    ) -> Result<WorkerDB, TdError> {
        seed_function_runner_worker(
            self.executor,
            execution,
            transaction,
            physical_function_run,
            status,
        )
        .await
    }
}
