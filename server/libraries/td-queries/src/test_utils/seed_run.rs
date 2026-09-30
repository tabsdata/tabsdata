//
// Copyright 2026 Tabsdata Inc.
//

//! Composite seed helpers used by the cross-project list-service tests:
//! one seeds a full run chain in a project, the other a user who may view
//! a single project.

use crate::test_utils::seed::SeedEntry;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::function::FunctionRegister;
use td_objects::dxo::user::UserDB;
use td_objects::types::basic::ReuseFrozen;
use td_objects::types::basic::{
    BundleId, CollectionName, Decorator, Description, EntityId, ExecutionId,
    LogicalFunctionRunStatus, PermissionType, ProjectId, RoleName, TransactionId, TransactionKey,
    UserEnabled, UserId, UserName, WorkerId, WorkerMessageStatus,
};

pub struct SeededRun {
    pub execution: ExecutionId,
    pub transaction: TransactionId,
    pub worker: WorkerId,
}

pub async fn seed_run(db: &TdPool, project: ProjectId) -> Result<SeededRun, TdError> {
    let collection = db
        .scoped(project)
        .seed()
        .collection(&CollectionName::try_from("collection")?, &UserId::admin())
        .await?;
    let create = FunctionRegister {
        parent_function: None,
        name: "joaquin".try_into()?,
        description: "f".try_into()?,
        bundle_id: BundleId::default(),
        snippet: "s".try_into()?,
        decorator: Decorator::Publisher,
        function_type: None,
        connector: None,
        connection: None,
        base_path: None,
        dependencies: None,
        triggers: None,
        tables: None,
        table_types: None,
        runtime_values: "mock runtime values".try_into()?,
        reuse_frozen_tables: ReuseFrozen::FALSE,
        enabled: None,
        workers: None,
    };
    let function_version = db
        .scoped(project)
        .seed()
        .function(&collection, &create)
        .await?;
    let execution = db
        .scoped(project)
        .seed()
        .execution(&function_version)
        .await?;
    let transaction = db
        .scoped(project)
        .seed()
        .transaction(&execution, &TransactionKey::try_from("ANY")?)
        .await?;
    let physical_function_run = db
        .scoped(project)
        .seed()
        .physical_function_run(
            &collection,
            &function_version,
            &execution,
            &transaction,
            &LogicalFunctionRunStatus::Scheduled,
        )
        .await?;
    let worker = db
        .scoped(project)
        .seed()
        .function_runner_worker(
            &execution,
            &transaction,
            &physical_function_run,
            &WorkerMessageStatus::Locked,
        )
        .await?;
    Ok(SeededRun {
        execution: execution.base.id,
        transaction: transaction.base.id,
        worker: worker.base.id,
    })
}

pub async fn seed_project_viewer(db: &TdPool, project: ProjectId) -> Result<UserDB, TdError> {
    let role = db
        .seed()
        .role(
            RoleName::try_from("viewer")?,
            Description::try_from("project viewer")?,
        )
        .await?;
    db.seed()
        .permission(
            PermissionType::view_project(),
            None,
            Some(EntityId::try_from(project)?),
            &role,
        )
        .await?;
    let user = db
        .seed()
        .user(&UserName::try_from("viewer")?, &UserEnabled::TRUE)
        .await?;
    db.seed().user_role(&user.id, &role.id).await?;
    Ok(user)
}
