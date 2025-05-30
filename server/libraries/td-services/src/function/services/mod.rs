//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::services::delete::DeleteFunctionService;
use crate::function::services::list::FunctionListService;
use crate::function::services::read::ReadFunctionService;
use crate::function::services::register::RegisterFunctionService;
use crate::function::services::update::UpdateFunctionService;
use crate::function::services::upload::UploadFunctionService;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{
    CreateRequest, DeleteRequest, ListRequest, ListResponse, ReadRequest, UpdateRequest,
};
use td_objects::rest_urls::{CollectionParam, FunctionParam};
use td_objects::sql::DaoQueries;
use td_objects::types::function::{
    Bundle, Function, FunctionRegister, FunctionUpdate, FunctionUpload, FunctionWithTables,
};
use td_objects::types::table::CollectionAtName;
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

pub(crate) mod delete;
pub(crate) mod list;
pub(crate) mod read;
pub(crate) mod register;
pub(crate) mod update;
pub(crate) mod upload;

pub struct FunctionServices {
    register: RegisterFunctionService,
    upload: UploadFunctionService,
    read_version: ReadFunctionService,
    list: FunctionListService,
    update: UpdateFunctionService,
    delete: DeleteFunctionService,
}

impl FunctionServices {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>, storage: Arc<Storage>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            register: RegisterFunctionService::new(db.clone(), authz_context.clone()),
            upload: UploadFunctionService::new(db.clone(), authz_context.clone(), storage.clone()),
            read_version: ReadFunctionService::new(db.clone(), authz_context.clone()),
            list: FunctionListService::new(db.clone(), queries.clone(), authz_context.clone()),
            update: UpdateFunctionService::new(db.clone(), authz_context.clone()),
            delete: DeleteFunctionService::new(db.clone(), authz_context.clone()),
        }
    }

    pub async fn register(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionRegister>, Function, TdError> {
        self.register.service().await
    }

    pub async fn upload(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionUpload>, Bundle, TdError> {
        self.upload.service().await
    }

    pub async fn read_version(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, FunctionWithTables, TdError> {
        self.read_version.service().await
    }

    pub async fn list(
        &self,
    ) -> TdBoxService<ListRequest<CollectionAtName>, ListResponse<Function>, TdError> {
        self.list.service().await
    }

    pub async fn update(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionParam, FunctionUpdate>, Function, TdError> {
        self.update.service().await
    }

    pub async fn delete(&self) -> TdBoxService<DeleteRequest<FunctionParam>, (), TdError> {
        self.delete.service().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::function::layers::register::{
        SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES,
    };
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::cte::CteQueries;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{
        DependencyStatus, FunctionStatus, TableDependency, TableName, TableStatus, TableTrigger,
        TriggerStatus, UserId,
    };
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::dependency::DependencyDBWithNames;
    use td_objects::types::function::{Function, FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::types::table::TableDB;
    use td_objects::types::trigger::TriggerDBWithNames;

    pub async fn assert_register(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        response: &Function,
    ) -> Result<(), TdError> {
        // Assertions
        let req_dependencies = create.dependencies().as_deref().unwrap_or(&[]);
        let req_triggers = create.triggers().as_deref().unwrap_or(&[]);
        let req_tables = create.tables().as_deref().unwrap_or(&[]);

        // Assert response is correct
        assert_eq!(response.collection_id(), collection.id());
        assert_eq!(response.name(), create.name());
        assert_eq!(response.description(), create.description());
        assert_eq!(*response.status(), FunctionStatus::Active);
        assert_eq!(response.decorator(), create.decorator());
        assert_eq!(response.bundle_id(), create.bundle_id());
        assert_eq!(response.snippet(), create.snippet());
        assert_eq!(response.defined_by_id(), user_id);
        assert_eq!(response.collection(), collection.name());

        let queries = DaoQueries::default();
        let function_version_id = response.id();

        // Assert function was created
        let functions: Vec<FunctionDB> = queries
            .select_by::<FunctionDB>(&function_version_id)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(functions.len(), 1);
        let function = &functions[0];
        assert_eq!(function.collection_id(), response.collection_id());
        assert_eq!(function.name(), response.name());
        assert_eq!(function.function_id(), response.function_id());
        assert_eq!(function.bundle_id(), response.bundle_id());
        assert_eq!(function.snippet(), response.snippet());
        assert_eq!(function.decorator(), response.decorator());
        assert_eq!(*function.status(), FunctionStatus::Active);

        // Assert table versions were created (query by active to filter deleted tables in updates)
        let table_versions: Vec<TableDB> = queries
            .select_by::<TableDB>(&(function_version_id, &TableStatus::Active))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(
            table_versions.len(),
            req_tables.len() + SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES.len()
        );
        for table in req_tables {
            let found = table_versions
                .iter()
                .find(|t| **t.name() == **table)
                .expect("table version not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(**found.name(), **table);
            assert_eq!(found.function_version_id(), function.id());
            assert!(found.function_param_pos().is_some());
            assert_eq!(found.defined_on(), function.defined_on());
            assert_eq!(found.defined_by_id(), function.defined_by_id());
        }

        // Assert dependency versions were created (query by active to filter deleted dependencies in updates)
        let dependency_versions: Vec<DependencyDBWithNames> = queries
            .select_by::<DependencyDBWithNames>(&(function_version_id, &DependencyStatus::Active))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(
            dependency_versions.len(),
            req_dependencies.len() + SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.len()
        );
        for dependency in req_dependencies {
            let found = dependency_versions
                .iter()
                .find(|d| **d.table_name() == **dependency.table())
                .expect("dependency version not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.function_version_id(), function.id());
            assert_eq!(found.function_id(), function.function_id());
            assert_eq!(
                found.table_collection(),
                dependency
                    .collection()
                    .as_ref()
                    .unwrap_or(collection.name())
            );
            assert_eq!(**found.table_name(), **dependency.table());
            assert_eq!(*found.table_versions(), dependency.versions().into());
            assert_eq!(found.defined_on(), function.defined_on());
            assert_eq!(found.defined_by_id(), function.defined_by_id());
            assert_eq!(*found.status(), DependencyStatus::Active);
        }

        // Assert trigger versions were created (query by active to filter deleted triggers in updates)
        let trigger_versions: Vec<TriggerDBWithNames> = queries
            .select_by::<TriggerDBWithNames>(&(function_version_id, &TriggerStatus::Active))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(trigger_versions.len(), req_triggers.len());
        for trigger in req_triggers {
            let found = trigger_versions
                .iter()
                .find(|d| **d.trigger_by_table_name() == **trigger.table())
                .expect("trigger version not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.function_version_id(), function.id());
            assert_eq!(found.function_id(), function.function_id());
            assert_eq!(
                found.trigger_by_collection(),
                trigger.collection().as_ref().unwrap_or(collection.name())
            );
            assert_eq!(**found.trigger_by_table_name(), **trigger.table());
            assert_eq!(*found.status(), TriggerStatus::Active);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn assert_update(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        created_function: &FunctionDB,
        update: &FunctionUpdate,
        response: &Function,
    ) -> Result<(), TdError> {
        // First, assert updated entities removed old ones
        let queries = DaoQueries::default();

        // Assert previous function version still exists (2 versions)
        let functions: Vec<FunctionDB> = queries
            .select_by::<FunctionDB>(&(created_function.function_id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(functions.len(), 2);

        // Assert new function version exists
        let functions: Vec<FunctionDB> = queries
            .select_by::<FunctionDB>(&(created_function.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(functions.len(), 1);
        assert_eq!(&functions[0], created_function);

        // Assert tables
        for table_dto in create.tables().as_deref().unwrap_or(&[]) {
            let table = &TableName::try_from(table_dto)?;
            // We will always have the old active version
            let old_version: Vec<TableDB> = queries
                .select_by::<TableDB>(&(collection.id(), table, created_function.id()))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TableStatus::Active);

            // And a new one, which will be active if still present, or else frozen
            let new_version: Vec<TableDB> = queries
                .select_by::<TableDB>(&(collection.id(), table, response.id()))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(new_version.len(), 1);
            let new_status = if update
                .tables()
                .as_deref()
                .unwrap_or(&[])
                .contains(table_dto)
            {
                TableStatus::Active
            } else {
                TableStatus::Frozen
            };
            assert_eq!(*new_version[0].status(), new_status);

            // Both with the same table_id
            assert_eq!(old_version[0].table_id(), new_version[0].table_id());
        }

        // Assert dependencies
        for dependency_dto in create.dependencies().as_deref().unwrap_or(&[]) {
            let dependency = &TableDependency::try_from(dependency_dto)?;
            // We will always have the old active version
            let old_version: Vec<DependencyDBWithNames> = queries
                .select_by::<DependencyDBWithNames>(&(
                    dependency
                        .collection()
                        .as_ref()
                        .unwrap_or(collection.name()),
                    dependency.table(),
                    created_function.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), DependencyStatus::Active);

            // And a new one, which will be active if still present, or else deleted
            let new_version: Vec<DependencyDBWithNames> = queries
                .select_by::<DependencyDBWithNames>(&(
                    dependency
                        .collection()
                        .as_ref()
                        .unwrap_or(collection.name()),
                    dependency.table(),
                    response.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(new_version.len(), 1);

            let new_status = if update
                .dependencies()
                .as_deref()
                .unwrap_or(&[])
                .contains(dependency_dto)
            {
                DependencyStatus::Active
            } else {
                DependencyStatus::Deleted
            };
            assert_eq!(*new_version[0].status(), new_status);

            // Both with the same dependency_id
            assert_eq!(
                old_version[0].dependency_id(),
                new_version[0].dependency_id()
            );
        }

        // Assert triggers
        for trigger_dto in create.triggers().as_deref().unwrap_or(&[]) {
            let trigger = TableTrigger::try_from(trigger_dto)?;
            // We will always have the old active version
            let old_version: Vec<TriggerDBWithNames> = queries
                .select_by::<TriggerDBWithNames>(&(
                    trigger.collection().as_ref().unwrap_or(collection.name()),
                    trigger.table(),
                    created_function.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TriggerStatus::Active);

            // And a new one, which will be active if still present, or else deleted
            let new_version: Vec<TriggerDBWithNames> = queries
                .select_by::<TriggerDBWithNames>(&(
                    trigger.collection().as_ref().unwrap_or(collection.name()),
                    trigger.table(),
                    response.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);

            let new_status = if update
                .triggers()
                .as_deref()
                .unwrap_or(&[])
                .contains(trigger_dto)
            {
                TriggerStatus::Active
            } else {
                TriggerStatus::Deleted
            };
            assert_eq!(*new_version[0].status(), new_status);

            // Both with the same trigger_id
            assert_eq!(old_version[0].trigger_id(), new_version[0].trigger_id());
        }

        // And finally, new version should be the exact same as if it just got registered.
        assert_register(db, user_id, collection, update, response).await?;
        Ok(())
    }

    pub async fn assert_delete(
        db: &DbPool,
        _user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        created_function: &FunctionDB,
    ) -> Result<(), TdError> {
        // First, assert updated entities removed old ones
        let queries = DaoQueries::default();

        // Assert new function version is deleted
        let functions: Vec<FunctionDB> = queries
            .select_versions_at::<FunctionDB>(None, None, &(created_function.function_id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(functions.len(), 1);
        assert_eq!(*functions[0].status(), FunctionStatus::Deleted);

        // Assert previous function version exists
        let functions: Vec<FunctionDB> = queries
            .select_by::<FunctionDB>(&(created_function.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(functions.len(), 1);
        assert_eq!(&functions[0], created_function);

        // Assert previous table versions do not have tables
        let tables: Vec<TableDB> = queries
            .select_versions_at::<TableDB>(None, None, &created_function.id())?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(tables.is_empty());

        // Assert tables
        for table_dto in create.tables().as_deref().unwrap_or(&[]) {
            let table = &TableName::try_from(table_dto)?;
            // We will always have the old active version
            let old_version: Vec<TableDB> = queries
                .select_by::<TableDB>(&(collection.id(), table, created_function.id()))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TableStatus::Active);
        }

        // Assert dependencies
        for dependency in create.dependencies().as_deref().unwrap_or(&[]) {
            let dependency = &TableDependency::try_from(dependency)?;
            // We will always have the old active version
            let old_version: Vec<DependencyDBWithNames> = queries
                .select_by::<DependencyDBWithNames>(&(
                    dependency
                        .collection()
                        .as_ref()
                        .unwrap_or(collection.name()),
                    dependency.table(),
                    created_function.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), DependencyStatus::Active);
        }

        // Assert triggers
        for trigger_dto in create.triggers().as_deref().unwrap_or(&[]) {
            let trigger = &TableTrigger::try_from(trigger_dto)?;
            // We will always have the old active version
            let old_version: Vec<TriggerDBWithNames> = queries
                .select_by::<TriggerDBWithNames>(&(
                    trigger.collection().as_ref().unwrap_or(collection.name()),
                    trigger.table(),
                    created_function.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TriggerStatus::Active);
        }

        Ok(())
    }
}
