//
// Copyright 2026 Tabsdata Inc.
//

//! Test-only seed surface. Reach via [`SeedEntry::seed`] on any
//! [`QueryExecutor`] (e.g. a `TdPool` in test code), then call the
//! per-DAO seed method:
//!
//! ```ignore
//! let project = db.seed().test_project().await?;
//! let collection = db.scoped(project.id).seed().collection(&name, &user).await?;
//! let function = db.scoped(project.id).seed().function(&collection, &create).await?;
//! ```
//!
//! Only the `test-utils` feature exposes this surface — production code
//! cannot reach it.

use crate::test_utils::seed_collection::{seed_collection, seed_collection_in_group};
use crate::test_utils::seed_execution::{seed_cron_execution, seed_execution};
use crate::test_utils::seed_function::{seed_child_function, seed_function};
use crate::test_utils::seed_group::seed_group;
use crate::test_utils::seed_logical_function_run::seed_logical_function_run;
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
use crate::test_utils::seed_user::{seed_system_user, seed_user};
use crate::test_utils::seed_user_role::seed_user_role;
use crate::test_utils::seed_worker::{seed_function_runner_worker, seed_worker};
use crate::test_utils::topology::topology_write;
use crate::{QueryExecutor, Writes};
use std::borrow::Borrow;
use ta_objects::errors::SqlError;
use td_database::access::{Holds, InProject, ProjectLock, RW, SeesProjectRows};
use td_database::domain::DomainConnection;
use td_database::domain::{Apiserver, Domained};
use td_database::pool::TdPool;
use td_database::pool::scoped::Scoped;
use td_error::TdError;
use td_objects::dxo::collection::CollectionDB;
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalFunctionRunInputDB};
use td_objects::dxo::permission::PermissionDB;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::project::ProjectDB;
use td_objects::dxo::role::RoleDB;
use td_objects::dxo::table::TableDB;
use td_objects::dxo::topology::TopologyInstant;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    CollectionName, DependencyPos, Description, EntityId, EntityName, FunctionId, GroupName,
    LogicalFunctionRunStatus, PermissionType, PhysicalFunctionRunStatus, ProjectId, ProjectName,
    RoleId, RoleName, TransactionKey, UserEnabled, UserId, UserName, VersionPos, WorkerStatus,
    WorkerType,
};

/// Entry trait providing [`seed`](SeedEntry::seed) on every
/// [`QueryExecutor`]. Deliberately separate from
/// [`Queries`](crate::Queries): the seed surface is `test-utils` only, and a
/// production import of one trait must not pull it in.
///
/// The handle adds **no** scope of its own — whether the writes are
/// project-scoped is the caller's choice of executor: `db.scoped(p).seed()`
/// scopes to `p`, a bare pool `seed()` does not, so field-less RLS-defaulted
/// seeds fail unless the caller scoped it.
pub trait SeedEntry: QueryExecutor {
    fn seed(self) -> Seed<Self> {
        Seed { executor: self }
    }
}

impl<E: QueryExecutor> SeedEntry for E {}

pub struct Seed<E> {
    pub(crate) executor: E,
}

impl<E: Writes<Domain = Apiserver, Scope: SeesProjectRows, Mode: Holds<ProjectLock>>> Seed<E> {
    // Project seeding uses its executor only to scope into the freshly-created
    // project, so the write bound is on the scoped view; `&TdPool` is the only
    // executor that scopes to one.
    pub async fn project(self, name: ProjectName) -> Result<ProjectDB, TdError>
    where
        for<'s> &'s Scoped<E, ProjectId>:
            Writes<Domain = Apiserver, Scope = InProject, Mode: Holds<ProjectLock>>,
    {
        seed_project(self.executor, name).await
    }

    pub async fn test_project(self) -> Result<ProjectDB, TdError>
    where
        for<'s> &'s Scoped<E, ProjectId>:
            Writes<Domain = Apiserver, Scope = InProject, Mode: Holds<ProjectLock>>,
    {
        seed_project(self.executor, ProjectName::try_from("test")?).await
    }

    pub async fn collection(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
    ) -> Result<CollectionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_collection(self.executor, collection_name, created_by).await
    }

    pub async fn collection_in_group(
        self,
        collection_name: &CollectionName,
        created_by: &UserId,
        group: &GroupDB,
    ) -> Result<CollectionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_collection_in_group(self.executor, collection_name, created_by, group).await
    }

    pub async fn execution(self, function_version: &FunctionDB) -> Result<ExecutionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_execution(self.executor, function_version).await
    }

    pub async fn cron_execution(self, function_version: &FunctionDB) -> Result<ExecutionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_cron_execution(self.executor, function_version).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn logical_input(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
        requirement_table_data_version: Option<&PhysicalTableDataVersionDB>,
        requirement_dependency_pos: Option<&DependencyPos>,
        requirement_version_pos: &VersionPos,
    ) -> Result<LogicalFunctionRunInputDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_logical_input(
            self.executor,
            collection,
            execution,
            transaction,
            physical_function_run,
            requirement_table_data_version,
            requirement_dependency_pos,
            requirement_version_pos,
        )
        .await
    }

    pub async fn logical_function_run(
        self,
        collection: &CollectionDB,
        function_version: &FunctionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        status: &LogicalFunctionRunStatus,
    ) -> Result<LogicalFunctionRunDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_logical_function_run(
            self.executor,
            collection,
            function_version,
            execution,
            transaction,
            status,
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
    ) -> Result<PhysicalFunctionRunDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_physical_function_run(
            self.executor,
            collection,
            function_version,
            execution,
            transaction,
            status,
            &PhysicalFunctionRunStatus::from(status),
        )
        .await
    }

    /// A run whose attempt is in `attempt_status`, for states the run's own
    /// status does not pin down (a planned run whose attempt was minted but
    /// not yet dispatched).
    pub async fn physical_function_run_with_attempt(
        self,
        collection: &CollectionDB,
        function_version: &FunctionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        status: &LogicalFunctionRunStatus,
        attempt_status: &PhysicalFunctionRunStatus,
    ) -> Result<PhysicalFunctionRunDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_physical_function_run(
            self.executor,
            collection,
            function_version,
            execution,
            transaction,
            status,
            attempt_status,
        )
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

    pub async fn group(self, name: GroupName, description: Description) -> Result<GroupDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_group(self.executor, name, description).await
    }

    pub async fn physical_table_data_version(
        self,
        collection: &CollectionDB,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
        table: &TableDB,
    ) -> Result<PhysicalTableDataVersionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
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
    ) -> Result<PhysicalTableDataVersionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
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
    ) -> Result<TransactionDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_transaction(self.executor, execution, transaction_key).await
    }

    pub async fn user(self, name: &UserName, enabled: &UserEnabled) -> Result<UserDB, TdError> {
        seed_user(self.executor, name, enabled).await
    }

    pub async fn system_user(self, name: &UserName) -> Result<UserDB, TdError> {
        seed_system_user(self.executor, name).await
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
    ) -> Result<WorkerDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_worker(
            self.executor,
            collection,
            function,
            worker_type,
            worker_status,
        )
        .await
    }

    pub async fn function_runner_worker(
        self,
        execution: &ExecutionDB,
        transaction: &TransactionDB,
        physical_function_run: &PhysicalFunctionRunDB,
    ) -> Result<WorkerDB, TdError>
    where
        E: Domained<Scope = InProject>,
    {
        seed_function_runner_worker(self.executor, execution, transaction, physical_function_run)
            .await
    }
}

/// Topology seeds write as one transaction, as the services do: each is one
/// topology version.
impl<P: Borrow<ProjectId> + Clone + Sync> Seed<&Scoped<&TdPool, P>> {
    async fn topology_write(
        &self,
    ) -> Result<
        (
            DomainConnection<Apiserver, InProject, RW<ProjectLock>>,
            TopologyInstant,
        ),
        TdError,
    > {
        let (project, pool) = self.executor.clone().into_parts();
        topology_write(pool, project.borrow()).await
    }

    pub async fn function(
        self,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
    ) -> Result<FunctionDB, TdError> {
        let (mut conn, topology_at) = self.topology_write().await?;
        let function = seed_function(&mut conn, topology_at, collection, function_create).await?;
        conn.commit().await.map_err(failed)?;
        Ok(function)
    }

    pub async fn child_function(
        self,
        collection: &CollectionDB,
        function_create: &FunctionRegister,
        parent: &FunctionId,
    ) -> Result<FunctionDB, TdError> {
        let (mut conn, topology_at) = self.topology_write().await?;
        let function =
            seed_child_function(&mut conn, topology_at, collection, function_create, parent)
                .await?;
        conn.commit().await.map_err(failed)?;
        Ok(function)
    }

    pub async fn system_tables(self, function: &FunctionDB) -> Result<Vec<TableDB>, TdError> {
        let (mut conn, topology_at) = self.topology_write().await?;
        let tables = seed_system_tables(&mut conn, topology_at, function).await?;
        conn.commit().await.map_err(failed)?;
        Ok(tables)
    }
}

fn failed(e: sqlx::Error) -> TdError {
    SqlError::AnySqlxError("seed".to_string(), e).into()
}
