//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::services::delete::DeleteFunctionService;
use crate::function::services::read::ReadFunctionService;
use crate::function::services::read_version::ReadFunctionVersionService;
use crate::function::services::register::RegisterFunctionService;
use crate::function::services::update::UpdateFunctionService;
use crate::function::services::upload::UploadFunctionService;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, DeleteRequest, ReadRequest, UpdateRequest};
use td_objects::rest_urls::{CollectionParam, FunctionParam, FunctionVersionParam};
use td_objects::types::function::{
    Bundle, FunctionRegister, FunctionUpdate, FunctionUpload, FunctionVersion,
    FunctionVersionWithAllVersions, FunctionVersionWithTables,
};
use td_storage::Storage;
use td_tower::service_provider::TdBoxService;

pub(crate) mod delete;
pub(crate) mod read;
pub(crate) mod read_version;
pub(crate) mod register;
pub(crate) mod update;
pub(crate) mod upload;

pub struct FunctionServices {
    register: RegisterFunctionService,
    upload: UploadFunctionService,
    read: ReadFunctionService,
    read_version: ReadFunctionVersionService,
    update: UpdateFunctionService,
    delete: DeleteFunctionService,
}

impl FunctionServices {
    pub fn new(db: DbPool, storage: Arc<Storage>) -> Self {
        Self {
            register: RegisterFunctionService::new(db.clone()),
            upload: UploadFunctionService::new(db.clone(), storage.clone()),
            read: ReadFunctionService::new(db.clone()),
            read_version: ReadFunctionVersionService::new(db.clone()),
            update: UpdateFunctionService::new(db.clone()),
            delete: DeleteFunctionService::new(db.clone()),
        }
    }

    pub async fn register(
        &self,
    ) -> TdBoxService<CreateRequest<CollectionParam, FunctionRegister>, FunctionVersion, TdError>
    {
        self.register.service().await
    }

    pub async fn upload(
        &self,
    ) -> TdBoxService<CreateRequest<FunctionParam, FunctionUpload>, Bundle, TdError> {
        self.upload.service().await
    }

    pub async fn read(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionParam>, FunctionVersionWithAllVersions, TdError> {
        self.read.service().await
    }

    pub async fn read_version(
        &self,
    ) -> TdBoxService<ReadRequest<FunctionVersionParam>, FunctionVersionWithTables, TdError> {
        self.read_version.service().await
    }

    pub async fn update(
        &self,
    ) -> TdBoxService<UpdateRequest<FunctionParam, FunctionUpdate>, FunctionVersion, TdError> {
        self.update.service().await
    }

    pub async fn delete(&self) -> TdBoxService<DeleteRequest<FunctionParam>, (), TdError> {
        self.delete.service().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{
        Decorator, DependencyStatus, Frozen, FunctionStatus, TableStatus, TriggerStatus, UserId,
    };
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::dependency::{DependencyDBWithNames, DependencyVersionDBWithNames};
    use td_objects::types::function::{
        FunctionDB, FunctionRegister, FunctionUpdate, FunctionVersion, FunctionVersionDB,
    };
    use td_objects::types::table::{TableDB, TableVersionDB};
    use td_objects::types::trigger::{TriggerDBWithNames, TriggerVersionDBWithNames};

    pub async fn assert_register(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        response: &FunctionVersion,
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
        let function_id = response.function_id();
        let function_version_id = response.id();

        // Assert function was created
        let function: FunctionDB = queries
            .select_by::<FunctionDB>(&function_id)?
            .build_query_as()
            .fetch_one(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(response.function_id(), function.id());
        assert_eq!(response.collection_id(), function.collection_id());
        assert_eq!(response.name(), function.name());
        assert_eq!(response.decorator(), function.decorator());
        assert_eq!(response.id(), function.function_version_id());
        assert_eq!(Frozen::from(false), *function.frozen());
        assert_eq!(response.defined_on(), function.created_on());
        assert_eq!(response.defined_by_id(), function.created_by_id());

        // Assert function version was created
        let function_versions: Vec<FunctionVersionDB> = queries
            .select_by::<FunctionVersionDB>(&function.function_version_id())?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_versions.len(), 1);
        let function_version = &function_versions[0];
        assert_eq!(function_version.collection_id(), function.collection_id());
        assert_eq!(function_version.name(), function.name());
        assert_eq!(function_version.runtime_values(), create.runtime_values());
        assert_eq!(function_version.function_id(), function.id());
        assert_eq!(function_version.bundle_id(), create.bundle_id());
        assert_eq!(function_version.snippet(), create.snippet());
        assert_eq!(function_version.decorator(), create.decorator());
        assert_eq!(function_version.defined_on(), function.created_on());
        assert_eq!(function_version.defined_by_id(), function.created_by_id());
        assert_eq!(*function_version.status(), FunctionStatus::Active);

        // Assert table versions were created (query by active to filter deleted tables in updates)
        let table_versions: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&(function_version_id, &TableStatus::Active))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(table_versions.len(), req_tables.len());
        for table in req_tables {
            let found = table_versions
                .iter()
                .find(|t| t.name() == table)
                .expect("table version not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.name(), table);
            assert_eq!(found.function_version_id(), function.function_version_id());
            assert!(found.function_param_pos().is_some());
            assert_eq!(found.defined_on(), function.created_on());
            assert_eq!(found.defined_by_id(), function.created_by_id());
        }

        // Assert tables were created (query by not frozen to filter deleted tables in updates)
        let tables: Vec<TableDB> = queries
            .select_by::<TableDB>(&(function_version_id, &Frozen::from(false)))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(tables.len(), req_tables.len());
        for table in req_tables {
            let found = tables
                .iter()
                .find(|t| t.name() == table)
                .expect("table not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.name(), table);
            assert_eq!(found.function_id(), function.id());
            assert_eq!(found.function_version_id(), function.function_version_id());
            assert_eq!(*found.frozen(), Frozen::from(false));
            assert_eq!(found.created_by_id(), function.created_by_id());
        }

        // Assert dependency versions were created (query by active to filter deleted dependencies in updates)
        let dependency_versions: Vec<DependencyVersionDBWithNames> = queries
            .select_by::<DependencyVersionDBWithNames>(&(
                function_version_id,
                &DependencyStatus::Active,
            ))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(dependency_versions.len(), req_dependencies.len());
        for dependency in req_dependencies {
            let found = dependency_versions
                .iter()
                .find(|d| d.table_name() == dependency.table())
                .expect("dependency version not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.function_id(), function.id());
            assert_eq!(found.function_version_id(), function.function_version_id());
            assert_eq!(
                found.table_collection(),
                dependency
                    .collection()
                    .as_ref()
                    .unwrap_or(collection.name())
            );
            assert_eq!(found.table_name(), dependency.table());
            assert_eq!(*found.table_versions(), dependency.versions().into());
            assert_eq!(found.defined_on(), function.created_on());
            assert_eq!(found.defined_by_id(), function.created_by_id());
            assert_eq!(*found.status(), DependencyStatus::Active);
        }

        // Assert dependencies were created
        let dependencies: Vec<DependencyDBWithNames> = queries
            .select_by::<DependencyDBWithNames>(&function_id)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(dependencies.len(), req_dependencies.len());
        for dependency in req_dependencies {
            let found = dependencies
                .iter()
                .find(|d| d.table_name() == dependency.table())
                .expect("dependency not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.function_id(), function.id());
            assert_eq!(
                found.table_collection(),
                dependency
                    .collection()
                    .as_ref()
                    .unwrap_or(collection.name())
            );
            assert_eq!(found.table_name(), dependency.table());
            assert_eq!(*found.table_versions(), dependency.versions().into());
        }

        // Assert trigger versions were created (query by active to filter deleted triggers in updates)
        let trigger_versions: Vec<TriggerVersionDBWithNames> = queries
            .select_by::<TriggerVersionDBWithNames>(&(function_version_id, &TriggerStatus::Active))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(trigger_versions.len(), req_triggers.len());
        for trigger in req_triggers {
            let found = trigger_versions
                .iter()
                .find(|d| d.trigger_by_table_name() == trigger.table())
                .expect("trigger version not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.function_id(), function.id());
            assert_eq!(found.function_version_id(), function.function_version_id());
            assert_eq!(
                found.trigger_by_collection(),
                trigger.collection().as_ref().unwrap_or(collection.name())
            );
            assert_eq!(found.trigger_by_table_name(), trigger.table());
            assert_eq!(*found.status(), TriggerStatus::Active);
        }

        // Assert triggers were created
        let triggers: Vec<TriggerDBWithNames> = queries
            .select_by::<TriggerDBWithNames>(&function_id)?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(triggers.len(), req_triggers.len());
        for trigger in req_triggers {
            let found = triggers
                .iter()
                .find(|d| d.trigger_by_table_name() == trigger.table())
                .expect("trigger not found");
            assert_eq!(found.collection_id(), function.collection_id());
            assert_eq!(found.function_id(), function.id());
            assert_eq!(
                found.trigger_by_collection(),
                trigger.collection().as_ref().unwrap_or(collection.name())
            );
            assert_eq!(found.trigger_by_table_name(), trigger.table());
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
        created_function_version: &FunctionVersionDB,
        update: &FunctionUpdate,
        response: &FunctionVersion,
    ) -> Result<(), TdError> {
        // First, assert updated entities removed old ones
        let queries = DaoQueries::default();

        // Assert function version does not have a function anymore
        let function: Option<FunctionDB> = queries
            .select_by::<FunctionDB>(&(created_function.function_version_id()))?
            .build_query_as()
            .fetch_optional(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(function.is_none());

        // Assert previous function version exists
        let function_versions: Vec<FunctionVersionDB> = queries
            .select_by::<FunctionVersionDB>(&(created_function_version.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_versions.len(), 1);
        assert_eq!(&function_versions[0], created_function_version);

        // Assert previous table versions do not have tables
        let tables: Vec<TableDB> = queries
            .select_by::<TableDB>(&created_function_version.id())?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(tables.is_empty());

        // Assert tables
        for table in create.tables().as_deref().unwrap_or(&[]) {
            // We will always have the old active version
            let old_version: Vec<TableVersionDB> = queries
                .select_by::<TableVersionDB>(&(
                    collection.id(),
                    table,
                    created_function_version.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TableStatus::Active);

            // And a new one, which will be active if still present, or else frozen
            let new_version: Vec<TableVersionDB> = queries
                .select_by::<TableVersionDB>(&(collection.id(), table, response.id()))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(new_version.len(), 1);
            let new_status = if update.tables().as_deref().unwrap_or(&[]).contains(table) {
                TableStatus::Active
            } else {
                TableStatus::Frozen
            };
            assert_eq!(*new_version[0].status(), new_status);

            // Both with the same table_id
            assert_eq!(old_version[0].table_id(), new_version[0].table_id());
        }

        // Assert dependencies
        for dependency in create.dependencies().as_deref().unwrap_or(&[]) {
            // We will always have the old active version
            let old_version: Vec<DependencyVersionDBWithNames> = queries
                .select_by::<DependencyVersionDBWithNames>(&(
                    dependency
                        .collection()
                        .as_ref()
                        .unwrap_or(collection.name()),
                    dependency.table(),
                    created_function_version.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), DependencyStatus::Active);

            // And a new one, which will be active if still present, or else deleted
            let new_version: Vec<DependencyVersionDBWithNames> = queries
                .select_by::<DependencyVersionDBWithNames>(&(
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
                .contains(dependency)
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
        for trigger in create.triggers().as_deref().unwrap_or(&[]) {
            // We will always have the old active version
            let old_version: Vec<TriggerVersionDBWithNames> = queries
                .select_by::<TriggerVersionDBWithNames>(&(
                    trigger.collection().as_ref().unwrap_or(collection.name()),
                    trigger.table(),
                    created_function_version.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TriggerStatus::Active);

            // And a new one, which will be active if still present, or else deleted
            let new_version: Vec<TriggerVersionDBWithNames> = queries
                .select_by::<TriggerVersionDBWithNames>(&(
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
                .contains(trigger)
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
        created_function_version: &FunctionVersionDB,
    ) -> Result<(), TdError> {
        // First, assert updated entities removed old ones
        let queries = DaoQueries::default();

        // Assert function version does not have a function anymore
        let function: Option<FunctionDB> = queries
            .select_by::<FunctionDB>(&(created_function.function_version_id()))?
            .build_query_as()
            .fetch_optional(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(function.is_none());

        // Assert previous function version exists
        let function_versions: Vec<FunctionVersionDB> = queries
            .select_by::<FunctionVersionDB>(&(created_function_version.id()))?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert_eq!(function_versions.len(), 1);
        assert_eq!(&function_versions[0], created_function_version);

        // Assert previous table versions do not have tables
        let tables: Vec<TableDB> = queries
            .select_by::<TableDB>(&created_function_version.id())?
            .build_query_as()
            .fetch_all(db)
            .await
            .map_err(handle_sql_err)?;
        assert!(tables.is_empty());

        // Assert tables
        for table in create.tables().as_deref().unwrap_or(&[]) {
            // We will always have the old active version
            let old_version: Vec<TableVersionDB> = queries
                .select_by::<TableVersionDB>(&(
                    collection.id(),
                    table,
                    created_function_version.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), TableStatus::Active);
        }

        // Assert dependencies
        for dependency in create.dependencies().as_deref().unwrap_or(&[]) {
            // We will always have the old active version
            let old_version: Vec<DependencyVersionDBWithNames> = queries
                .select_by::<DependencyVersionDBWithNames>(&(
                    dependency
                        .collection()
                        .as_ref()
                        .unwrap_or(collection.name()),
                    dependency.table(),
                    created_function_version.id(),
                ))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(*old_version[0].status(), DependencyStatus::Active);
        }

        // Assert triggers
        for trigger in create.triggers().as_deref().unwrap_or(&[]) {
            // We will always have the old active version
            let old_version: Vec<TriggerVersionDBWithNames> = queries
                .select_by::<TriggerVersionDBWithNames>(&(
                    trigger.collection().as_ref().unwrap_or(collection.name()),
                    trigger.table(),
                    created_function_version.id(),
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
