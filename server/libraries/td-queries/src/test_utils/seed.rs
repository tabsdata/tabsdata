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
use crate::test_utils::seed_project::seed_project;
use crate::test_utils::seed_role::seed_role;
use crate::test_utils::seed_system_tables::seed_system_tables;
use crate::test_utils::seed_transaction::seed_transaction;
use crate::test_utils::seed_user::seed_user;
use crate::test_utils::seed_user_role::seed_user_role;
use crate::test_utils::seed_worker::{seed_function_runner_worker, seed_worker};
use td_database::dynamic::DynamicConnection;
use td_database::pool::TdPool;
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
use td_objects::dxo::project::ProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    CollectionId, CollectionName, DependencyPos, Description, EntityId, EntityName, GroupId,
    GroupName, LogicalFunctionRunStatus, PermissionType, ProjectId, ProjectName, RoleId, RoleName,
    ToCollectionId, TransactionKey, UserEnabled, UserId, UserName, VersionPos, WorkerMessageStatus,
    WorkerStatus, WorkerType,
};
use td_tower::error::ConnectionError;

query_handle! {
    /// Entry trait providing [`seed`](SeedEntry::seed) on every
    /// [`QueryExecutor`](crate::QueryExecutor). Returns a [`Seed`] handle.
    pub trait SeedEntry {
        fn seed(self) -> Seed<Self> {
            Seed {
                executor: self,
                project: td_objects::types::basic::ProjectId::sandbox(),
            }
        }
    }
}

pub struct Seed<E> {
    pub(crate) executor: E,
    /// Project the field-less seeds scope their writes to. Defaults to
    /// `sandbox`; override with [`Seed::in_project`].
    pub(crate) project: ProjectId,
}

// Seeds operate on a pool: the field-less ones scope their writes via
// `in_project` (which must `begin` a transaction), so the executor is the
// concrete `&TdPool` rather than an arbitrary `QueryExecutor`.
impl<'a> Seed<&'a TdPool> {
    /// Target a project for this handle's field-less seeds (default `sandbox`).
    /// `db.seed().in_project(p).collection(..)` lands the collection in `p`.
    pub fn in_project(mut self, project: ProjectId) -> Self {
        self.project = project;
        self
    }

    /// Run `f` in a transaction scoped to this handle's project (`SET LOCAL
    /// app.project_id`), committing on `Ok` and rolling back on `Err`. Field-
    /// less seeds route through here so their rows default `project_id` to it;
    /// `seed_project` reuses it (with `in_project(new_project)`) for the system
    /// groups of a freshly-created project. No-op scope on non-PostgreSQL.
    pub(crate) async fn scoped<F, T>(&self, f: F) -> Result<T, TdError>
    where
        F: AsyncFnOnce(&mut DynamicConnection<'_>) -> Result<T, TdError>,
    {
        let tx = self
            .executor
            .begin()
            .await
            .map_err(ConnectionError::CannotBeginTransaction)?;
        // Own the transaction inside the connection so it can be recovered and
        // committed once the closure's borrow ends.
        let mut conn = tx.into_connection();
        conn.set_project_scope(&self.project.to_string())
            .await
            .map_err(ConnectionError::CannotSetProjectScope)?;

        match f(&mut conn).await {
            Ok(value) => {
                let tx = conn
                    .into_transaction()
                    .expect("scoped connection always owns its transaction");
                tx.commit()
                    .await
                    .map_err(ConnectionError::CannotCommitTransaction)?;
                Ok(value)
            }
            Err(err) => {
                if let Some(tx) = conn.into_transaction() {
                    let _ = tx.rollback().await;
                }
                Err(err)
            }
        }
    }

    pub async fn collection(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
    ) -> Result<CollectionDB, TdError> {
        self.scoped(async |conn| seed_collection(conn, collection_name, created_by).await)
            .await
    }

    pub async fn collection_in_group(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
        group_id: GroupId,
    ) -> Result<CollectionDB, TdError> {
        self.scoped(async |conn| {
            seed_collection_in_group(conn, collection_name, created_by, group_id).await
        })
        .await
    }

    pub async fn execution(self, function_version: &FunctionDB) -> Result<ExecutionDB, TdError> {
        self.scoped(async |conn| seed_execution(conn, function_version).await)
            .await
    }

    pub async fn cron_execution(
        self,
        function_version: &FunctionDB,
    ) -> Result<ExecutionDB, TdError> {
        self.scoped(async |conn| seed_cron_execution(conn, function_version).await)
            .await
    }

    pub async fn function(
        self,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
    ) -> Result<FunctionDB, TdError> {
        self.scoped(async |conn| seed_function(conn, collection, function_create).await)
            .await
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
        self.scoped(async |conn| {
            seed_logical_input(
                conn,
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
        })
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
        self.scoped(async |conn| {
            seed_inter_collection_permission(conn, from_collection, to_collection).await
        })
        .await
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

    pub async fn project(self, name: ProjectName) -> Result<ProjectDB, TdError> {
        seed_project(self.executor, name).await
    }

    pub async fn group(
        self,
        name: GroupName,
        description: Description,
    ) -> Result<GroupDB, TdError> {
        self.scoped(async |conn| seed_group(conn, name, description).await)
            .await
    }

    pub async fn system_tables(self, function: &FunctionDB) -> Result<Vec<TableDB>, TdError> {
        self.scoped(async |conn| seed_system_tables(conn, function).await)
            .await
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
        self.scoped(async |conn| seed_transaction(conn, execution, transaction_key).await)
            .await
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
