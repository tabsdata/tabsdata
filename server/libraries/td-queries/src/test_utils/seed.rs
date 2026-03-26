//
// Copyright 2026 Tabs Data Inc.
//

use crate::test_utils::seed_collection::seed_collection;
use crate::test_utils::seed_execution::{seed_cron_execution, seed_execution};
use crate::test_utils::seed_function::seed_function;
use crate::test_utils::seed_function_requirement::seed_function_requirement;
use crate::test_utils::seed_function_run::seed_function_run;
use crate::test_utils::seed_inter_collection_permission::seed_inter_collection_permission;
use crate::test_utils::seed_permission::seed_permission;
use crate::test_utils::seed_role::seed_role;
use crate::test_utils::seed_system_tables::seed_system_tables;
use crate::test_utils::seed_table_data_version::{
    seed_table_data_version, seed_table_data_version_with_data,
};
use crate::test_utils::seed_transaction::seed_transaction;
use crate::test_utils::seed_user::seed_user;
use crate::test_utils::seed_user_role::seed_user_role;
use crate::test_utils::seed_worker::{seed_function_runner_worker, seed_worker};
use td_database::sea_query::executor::SeaExecutor;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::function_requirement::FunctionRequirementDB;
use td_objects::dxo::function_run::FunctionRunDB;
use td_objects::dxo::inter_collection_permission::InterCollectionPermissionDB;
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::table_data_version::TableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    CollectionId, CollectionName, DependencyPos, Description, EntityId, EntityName,
    FunctionRunStatus, PermissionType, RoleId, RoleName, ToCollectionId, TransactionKey,
    UserEnabled, UserId, UserName, VersionPos, WorkerMessageStatus, WorkerStatus, WorkerType,
};

pub trait SeedQueries {
    fn seed_collection(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
    ) -> impl Future<Output = Result<CollectionDB, TdError>>;

    fn seed_execution(
        self,
        function_version: &FunctionDB,
    ) -> impl Future<Output = Result<ExecutionDB, TdError>>;

    fn seed_cron_execution(
        self,
        function_version: &FunctionDB,
    ) -> impl Future<Output = Result<ExecutionDB, TdError>>;

    fn seed_function(
        self,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
    ) -> impl Future<Output = Result<FunctionDB, TdError>>;

    #[allow(clippy::too_many_arguments)]
    fn seed_function_requirement(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        requirement_table: &TableDB,
        requirement_function_run: Option<&FunctionRunDB>,
        requirement_table_data_version: Option<&TableDataVersionDB>,
        requirement_dependency_pos: Option<&DependencyPos>,
        requirement_version_pos: &VersionPos,
    ) -> impl Future<Output = Result<FunctionRequirementDB, TdError>>;

    fn seed_function_run(
        self,
        collection: &CollectionDB,
        function_version: &FunctionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        status: &FunctionRunStatus,
    ) -> impl Future<Output = Result<FunctionRunDB, TdError>>;

    fn seed_inter_collection_permission(
        self,
        from_collection: &CollectionId,
        to_collection: &ToCollectionId,
    ) -> impl Future<Output = Result<InterCollectionPermissionDB, TdError>>;

    fn seed_permission(
        self,
        permission_type: PermissionType,
        entity_name: Option<EntityName>,
        entity_id: Option<EntityId>,
        role_db: &RoleDB,
    ) -> impl Future<Output = Result<PermissionDB, TdError>>;

    fn seed_role(
        self,
        name: RoleName,
        description: Description,
    ) -> impl Future<Output = Result<RoleDB, TdError>>;

    fn seed_system_tables(
        self,
        function: &FunctionDB,
    ) -> impl Future<Output = Result<Vec<TableDB>, TdError>>;

    fn seed_table_data_version(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        table: &TableDB,
    ) -> impl Future<Output = Result<TableDataVersionDB, TdError>>;

    fn seed_table_data_version_with_data(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        table: &TableDB,
    ) -> impl Future<Output = Result<TableDataVersionDB, TdError>>;

    fn seed_transaction(
        self,
        execution: &ExecutionDB,
        transaction_key: &TransactionKey,
    ) -> impl Future<Output = Result<TransactionDB, TdError>>;

    fn seed_user(
        self,
        name: &UserName,
        enabled: &UserEnabled,
    ) -> impl Future<Output = Result<UserDB, TdError>>;

    fn seed_user_role(
        self,
        user: &UserId,
        role: &RoleId,
    ) -> impl Future<Output = Result<UserRoleDB, TdError>>;

    fn seed_worker(
        self,
        collection: &CollectionDB,
        function: &FunctionDB,
        worker_type: &WorkerType,
        worker_status: &WorkerStatus,
        message_status: &WorkerMessageStatus,
    ) -> impl Future<Output = Result<WorkerDB, TdError>>;

    fn seed_function_runner_worker(
        self,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        status: &WorkerMessageStatus,
    ) -> impl Future<Output = Result<WorkerDB, TdError>>;
}

impl<E> SeedQueries for E
where
    E: SeaExecutor + Send + Copy,
{
    fn seed_collection(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
    ) -> impl Future<Output = Result<CollectionDB, TdError>> {
        seed_collection(self, collection_name, created_by)
    }

    fn seed_execution(
        self,
        function_version: &FunctionDB,
    ) -> impl Future<Output = Result<ExecutionDB, TdError>> {
        seed_execution(self, function_version)
    }

    fn seed_cron_execution(
        self,
        function_version: &FunctionDB,
    ) -> impl Future<Output = Result<ExecutionDB, TdError>> {
        seed_cron_execution(self, function_version)
    }

    fn seed_function(
        self,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
    ) -> impl Future<Output = Result<FunctionDB, TdError>> {
        seed_function(self, collection, function_create)
    }

    fn seed_function_requirement(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        requirement_table: &TableDB,
        requirement_function_run: Option<&FunctionRunDB>,
        requirement_table_data_version: Option<&TableDataVersionDB>,
        requirement_dependency_pos: Option<&DependencyPos>,
        requirement_version_pos: &VersionPos,
    ) -> impl Future<Output = Result<FunctionRequirementDB, TdError>> {
        seed_function_requirement(
            self,
            collection,
            execution,
            transaction,
            function_run,
            requirement_table,
            requirement_function_run,
            requirement_table_data_version,
            requirement_dependency_pos,
            requirement_version_pos,
        )
    }

    fn seed_function_run(
        self,
        collection: &CollectionDB,
        function_version: &FunctionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        status: &FunctionRunStatus,
    ) -> impl Future<Output = Result<FunctionRunDB, TdError>> {
        seed_function_run(
            self,
            collection,
            function_version,
            execution,
            transaction,
            status,
        )
    }

    fn seed_inter_collection_permission(
        self,
        from_collection: &CollectionId,
        to_collection: &ToCollectionId,
    ) -> impl Future<Output = Result<InterCollectionPermissionDB, TdError>> {
        seed_inter_collection_permission(self, from_collection, to_collection)
    }

    fn seed_permission(
        self,
        permission_type: PermissionType,
        entity_name: Option<EntityName>,
        entity_id: Option<EntityId>,
        role_db: &RoleDB,
    ) -> impl Future<Output = Result<PermissionDB, TdError>> {
        seed_permission(self, permission_type, entity_name, entity_id, role_db)
    }

    fn seed_role(
        self,
        name: RoleName,
        description: Description,
    ) -> impl Future<Output = Result<RoleDB, TdError>> {
        seed_role(self, name, description)
    }

    fn seed_system_tables(
        self,
        function: &FunctionDB,
    ) -> impl Future<Output = Result<Vec<TableDB>, TdError>> {
        seed_system_tables(self, function)
    }

    fn seed_table_data_version(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        table: &TableDB,
    ) -> impl Future<Output = Result<TableDataVersionDB, TdError>> {
        seed_table_data_version(
            self,
            collection,
            execution,
            transaction,
            function_run,
            table,
        )
    }

    fn seed_table_data_version_with_data(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        table: &TableDB,
    ) -> impl Future<Output = Result<TableDataVersionDB, TdError>> {
        seed_table_data_version_with_data(
            self,
            collection,
            execution,
            transaction,
            function_run,
            table,
        )
    }

    fn seed_transaction(
        self,
        execution: &ExecutionDB,
        transaction_key: &TransactionKey,
    ) -> impl Future<Output = Result<TransactionDB, TdError>> {
        seed_transaction(self, execution, transaction_key)
    }

    fn seed_user(
        self,
        name: &UserName,
        enabled: &UserEnabled,
    ) -> impl Future<Output = Result<UserDB, TdError>> {
        seed_user(self, name, enabled)
    }

    fn seed_user_role(
        self,
        user: &UserId,
        role: &RoleId,
    ) -> impl Future<Output = Result<UserRoleDB, TdError>> {
        seed_user_role(self, user, role)
    }

    fn seed_worker(
        self,
        collection: &CollectionDB,
        function: &FunctionDB,
        worker_type: &WorkerType,
        worker_status: &WorkerStatus,
        message_status: &WorkerMessageStatus,
    ) -> impl Future<Output = Result<WorkerDB, TdError>> {
        seed_worker(
            self,
            collection,
            function,
            worker_type,
            worker_status,
            message_status,
        )
    }

    fn seed_function_runner_worker(
        self,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        function_run: &FunctionRunDB,
        status: &WorkerMessageStatus,
    ) -> impl Future<Output = Result<WorkerDB, TdError>> {
        seed_function_runner_worker(self, execution, transaction, function_run, status)
    }
}
