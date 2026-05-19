//
// Copyright 2025 Tabs Data Inc.
//

use crate::function::services::delete::DeleteFunctionService;
use crate::function::services::enable::EnableFunctionService;
use crate::function::services::history::FunctionHistoryService;
use crate::function::services::list::FunctionListService;
use crate::function::services::list_by_collection::FunctionListByCollectionService;
use crate::function::services::list_history::FunctionListHistoryService;
use crate::function::services::list_history_by_collection::FunctionListHistoryByCollectionService;
use crate::function::services::list_patches::ListFunctionPatchesService;
use crate::function::services::patch::CreateFunctionPatchService;
use crate::function::services::read::ReadFunctionService;
use crate::function::services::register::RegisterFunctionService;
use crate::function::services::update::UpdateFunctionService;
use crate::function::services::upload::UploadFunctionService;
use getset::Getters;
use ta_services::factory::ServiceFactory;

pub(crate) mod delete;
pub(crate) mod enable;
pub(crate) mod history;
pub(crate) mod list;
pub(crate) mod list_by_collection;
pub(crate) mod list_history;
pub(crate) mod list_history_by_collection;
pub(crate) mod list_patches;
pub(crate) mod patch;
pub(crate) mod read;
pub(crate) mod register;
pub(crate) mod update;
pub(crate) mod upload;

#[derive(ServiceFactory, Getters)]
#[getset(get = "pub")]
pub struct FunctionServices {
    pub register: RegisterFunctionService,
    pub upload: UploadFunctionService,
    pub read_version: ReadFunctionService,
    pub list_by_collection: FunctionListByCollectionService,
    pub list: FunctionListService,
    pub list_history: FunctionListHistoryService,
    pub list_history_by_collection: FunctionListHistoryByCollectionService,
    pub update: UpdateFunctionService,
    pub enable: EnableFunctionService,
    pub delete: DeleteFunctionService,
    pub history: FunctionHistoryService,
    pub patch: CreateFunctionPatchService,
    pub list_patches: ListFunctionPatchesService,
}

#[cfg(test)]
pub(crate) mod tests {
    use chrono::DateTime;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::dependency::DependencyDBWithNames;
    use td_objects::dxo::function::{Function, FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::dxo::table::{TableDB, TableDBWithNames};
    use td_objects::dxo::trigger::TriggerDBWithNames;
    use td_objects::system::{
        SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES,
    };
    use td_objects::types::basic::{
        AtTime, DependencyStatus, Enabled, FunctionStatus, TableName, TableStatus, TriggerStatus,
        UserId,
    };
    use td_objects::types::composed::{TableDependency, TableTrigger, TableTriggerDto};
    use td_objects::types::table_ref::TableRef;
    use td_queries::{CurrentEntry, HistoricEntry, LiveEntry};
    pub async fn assert_register(
        db: &TdPool,
        user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        response: &Function,
    ) -> Result<(), TdError> {
        // Assertions
        let req_tables = create.tables.as_deref().unwrap_or(&[]);
        let req_dependencies = create.dependencies.as_deref().unwrap_or(&[]);
        // Similar to build_trigger_versions
        let req_triggers: Vec<_> = if let Some(req_triggers) = create.triggers.as_deref() {
            req_triggers.to_vec()
        } else {
            req_dependencies
                .iter()
                .filter(|t| {
                    !((t.collection.is_none()
                        || t.collection
                            .as_ref()
                            .map(|c| c == &collection.name)
                            .unwrap_or(false))
                        && req_tables.contains(&t.table))
                })
                .map(|t| TableTriggerDto::new(TableRef::new(t.collection.clone(), t.table.clone())))
                .collect()
        };

        // Assert response is correct
        assert_eq!(response.collection_id, collection.id);
        assert_eq!(response.name, create.name);
        assert_eq!(response.description, create.description);
        assert_eq!(response.status, FunctionStatus::Active);
        assert_eq!(response.decorator, create.decorator);
        assert_eq!(response.bundle_id, create.bundle_id);
        assert_eq!(response.snippet, create.snippet);
        assert_eq!(response.defined_by_id, *user_id);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.enabled, Enabled::TRUE);

        let function_version_id = response.id.clone();
        let function_id = response.function_id;

        // Assert function was created
        let functions: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&function_version_id)
            .await?;
        assert_eq!(functions.len(), 1);
        let function = &functions[0];
        assert_eq!(function.collection_id, response.collection_id);
        assert_eq!(function.name, response.name);
        assert_eq!(function.function_id, response.function_id);
        assert_eq!(function.bundle_id, response.bundle_id);
        assert_eq!(function.snippet, response.snippet);
        assert_eq!(function.decorator, response.decorator);
        assert_eq!(function.status, FunctionStatus::Active);

        // Assert table versions were created
        let table_versions: Vec<TableDB> = db
            .current()
            .state(TableDB::active())
            .fetch_all(&function_id)
            .await?;
        assert_eq!(
            table_versions.len(),
            req_tables.len() + SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES.len()
        );
        for table in req_tables {
            let found = table_versions
                .iter()
                .find(|t| **t.name == **table)
                .expect("table version not found");
            assert_eq!(found.collection_id, function.collection_id);
            assert_eq!(**found.name, **table);
            assert_eq!(found.function_version_id, function.id);
            assert!(found.function_param_pos.is_some());
            assert_eq!(found.defined_on, function.defined_on);
            assert_eq!(found.defined_by_id, function.defined_by_id);
            assert_eq!(found.status, TableStatus::Active);
        }

        // Assert dependency versions were created
        let dependency_versions: Vec<DependencyDBWithNames> = db
            .current()
            .state(DependencyDBWithNames::active())
            .fetch_all(&function_id)
            .await?;
        assert_eq!(
            dependency_versions.len(),
            req_dependencies.len() + SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.len()
        );
        for dependency in req_dependencies {
            let dependency = &TableDependency::try_from(dependency)?;
            let dependency_table = db
                .current()
                .fetch_one::<TableDBWithNames>(and!(
                    &dependency.collection.as_ref().unwrap_or(&collection.name),
                    &dependency.table,
                ))
                .await?;
            let found = dependency_versions
                .iter()
                .find(|d| d.table_id == dependency_table.table_id)
                .expect("dependency version not found");
            assert_eq!(found.collection_id, function.collection_id);
            assert_eq!(found.function_id, function.function_id);
            assert_eq!(
                found.table_collection,
                *dependency.collection.as_ref().unwrap_or(&collection.name)
            );
            assert_eq!(*found.table_versions, dependency.versions);
            assert_eq!(found.defined_on, function.defined_on);
            assert_eq!(found.defined_by_id, function.defined_by_id);
            assert_eq!(found.status, DependencyStatus::Active);
        }

        // Assert trigger versions were created
        let trigger_versions: Vec<TriggerDBWithNames> = db
            .current()
            .state(TriggerDBWithNames::active())
            .fetch_all(&function_id)
            .await?;
        assert_eq!(trigger_versions.len(), req_triggers.len());
        for trigger in req_triggers {
            let trigger = TableTrigger::try_from(trigger)?;
            let trigger_table = db
                .current()
                .fetch_one::<TableDBWithNames>(and!(
                    trigger.collection.as_ref().unwrap_or(&collection.name),
                    &trigger.table,
                ))
                .await?;
            let found = trigger_versions
                .iter()
                .find(|d| d.trigger_by_table_id == trigger_table.table_id)
                .expect("trigger version not found");
            assert_eq!(found.collection_id, function.collection_id);
            assert_eq!(found.function_id, function.function_id);
            assert_eq!(
                found.trigger_by_collection,
                *trigger.collection.as_ref().unwrap_or(&collection.name)
            );
            assert_eq!(found.status, TriggerStatus::Active);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn assert_update(
        db: &TdPool,
        user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        created_function: &FunctionDB,
        update: &FunctionUpdate,
        response: &Function,
    ) -> Result<(), TdError> {
        // First, assert updated entities removed old ones

        // Assert previous function version still exists (2 versions)
        let functions: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&created_function.function_id)
            .await?;
        assert_eq!(functions.len(), 2);

        // Assert new function version exists
        let functions: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&created_function.id)
            .await?;
        assert_eq!(functions.len(), 1);
        assert_eq!(&functions[0], created_function);

        // Assert tables
        for table_dto in create.tables.as_deref().unwrap_or(&[]) {
            let table = &TableName::try_from(table_dto)?;
            // We will always have the old active version
            let old_version = db
                .live()
                .fetch_all::<TableDB>(and!(collection.id, table, &created_function.id))
                .await?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(old_version[0].status, TableStatus::Active);

            // And a new one, which will be active if still present, or else frozen
            let new_version = db
                .live()
                .fetch_all::<TableDB>(and!(collection.id, table, &response.id))
                .await?;
            assert_eq!(new_version.len(), 1);
            let new_status = if update.tables.as_deref().unwrap_or(&[]).contains(table_dto) {
                TableStatus::Active
            } else {
                TableStatus::Frozen
            };
            assert_eq!(new_version[0].status, new_status);

            // Both with the same table_id
            assert_eq!(old_version[0].table_id, new_version[0].table_id);
        }

        // Use at time of the function version minus 1ms (right before the function version was defined)
        let before_at_time = AtTime::try_from(
            DateTime::from_timestamp_millis(response.defined_on.timestamp_millis() - 1).unwrap(),
        )?;

        // Assert dependencies
        for dependency_dto in create.dependencies.as_deref().unwrap_or(&[]) {
            let dependency = &TableDependency::try_from(dependency_dto)?;

            let dependency_table: TableDBWithNames = db
                .historic(&before_at_time)
                .fetch_one::<TableDBWithNames>(&dependency.table)
                .await?;
            // We will always have the old active version
            let old_version = db
                .historic(&before_at_time)
                .fetch_all::<DependencyDBWithNames>(and!(
                    dependency_table.table_id,
                    created_function.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(old_version[0].status, DependencyStatus::Active);

            // And a new one, which will be active if still present, or else deleted
            let dependency_table = db
                .current()
                .fetch_one::<TableDBWithNames>(&dependency.table)
                .await?;
            let new_version = db
                .current()
                .fetch_all::<DependencyDBWithNames>(and!(
                    dependency_table.table_id,
                    response.function_id
                ))
                .await?;
            assert_eq!(new_version.len(), 1);

            let new_status = if update
                .dependencies
                .as_deref()
                .unwrap_or(&[])
                .contains(dependency_dto)
            {
                DependencyStatus::Active
            } else {
                DependencyStatus::Deleted
            };
            assert_eq!(new_version[0].status, new_status);

            // Both with the same dependency_id
            assert_eq!(old_version[0].dependency_id, new_version[0].dependency_id);
        }

        // Assert triggers
        for trigger_dto in create.triggers.as_deref().unwrap_or(&[]) {
            let trigger = TableTrigger::try_from(trigger_dto)?;
            let trigger_table: TableDBWithNames = db
                .current()
                .fetch_one::<TableDBWithNames>(&trigger.table)
                .await?;
            // We will always have the old active version
            let old_version: Vec<TriggerDBWithNames> = db
                .historic(&before_at_time)
                .fetch_all::<TriggerDBWithNames>(and!(
                    trigger_table.table_id,
                    created_function.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);

            // And a new one, which will be active if still present, or else deleted
            let new_version = db
                .current()
                .fetch_all::<TriggerDBWithNames>(and!(trigger_table.table_id, response.function_id))
                .await?;
            assert_eq!(old_version.len(), 1);

            let new_status = if update
                .triggers
                .as_deref()
                .unwrap_or(&[])
                .contains(trigger_dto)
            {
                TriggerStatus::Active
            } else {
                TriggerStatus::Deleted
            };
            assert_eq!(new_version[0].status, new_status);

            // Both with the same trigger_id
            assert_eq!(old_version[0].trigger_id, new_version[0].trigger_id);
        }

        // And finally, new version should be the exact same as if it just got registered.
        let create = create.clone();
        let update = update.clone();
        let update_as_register = FunctionRegister {
            name: update.name.unwrap_or(create.name),
            description: update.description.unwrap_or(create.description),
            bundle_id: update.bundle_id.unwrap_or(create.bundle_id),
            snippet: update.snippet.unwrap_or(create.snippet),
            decorator: update.decorator.unwrap_or(create.decorator),
            connector: update.connector.unwrap_or(create.connector),
            dependencies: update.dependencies,
            triggers: update.triggers,
            tables: update.tables,
            table_types: update.table_types,
            runtime_values: update.runtime_values.unwrap_or(create.runtime_values),
            reuse_frozen_tables: update.reuse_frozen_tables,
            enabled: update.enabled,
            workers: None,
        };
        assert_register(db, user_id, collection, &update_as_register, response).await?;
        Ok(())
    }

    pub async fn assert_delete(
        db: &TdPool,
        _user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionRegister,
        created_function: &FunctionDB,
    ) -> Result<(), TdError> {
        // First, assert updated entities removed old ones

        // Assert new function version is deleted
        let functions: Vec<FunctionDB> = db
            .current()
            .fetch_all::<FunctionDB>(&created_function.function_id)
            .await?;
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].status, FunctionStatus::Deleted);

        // Assert previous function version exists
        let functions: Vec<FunctionDB> = db
            .live()
            .fetch_all::<FunctionDB>(&created_function.id)
            .await?;
        assert_eq!(functions.len(), 1);
        assert_eq!(&functions[0], created_function);

        // Assert previous table versions do not have tables
        let tables: Vec<TableDB> = db
            .current()
            .fetch_all::<TableDB>(&created_function.id)
            .await?;
        assert!(tables.is_empty());

        // Assert tables
        for table_dto in create.tables.as_deref().unwrap_or(&[]) {
            let table = &TableName::try_from(table_dto)?;
            // We will always have the old active version
            let old_version = db
                .live()
                .fetch_all::<TableDB>(and!(collection.id, table, &created_function.id))
                .await?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(old_version[0].status, TableStatus::Active);
        }

        // Assert dependencies
        for dependency in create.dependencies.as_deref().unwrap_or(&[]) {
            let dependency = &TableDependency::try_from(dependency)?;
            let table_dependency = db
                .historic(&created_function.defined_on)
                .fetch_one::<TableDBWithNames>(&dependency.table)
                .await?;
            // We will always have the old active version
            let old_version = db
                .historic(&created_function.defined_on)
                .fetch_all::<DependencyDBWithNames>(and!(
                    table_dependency.table_id,
                    created_function.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(old_version[0].status, DependencyStatus::Active);
        }

        // Assert triggers
        for trigger_dto in create.triggers.as_deref().unwrap_or(&[]) {
            let trigger = &TableTrigger::try_from(trigger_dto)?;
            let table_trigger = db
                .historic(&created_function.defined_on)
                .fetch_one::<TableDBWithNames>(&trigger.table)
                .await?;
            // We will always have the old active version
            let old_version = db
                .historic(&created_function.defined_on)
                .fetch_all::<TriggerDBWithNames>(and!(
                    table_trigger.table_id,
                    created_function.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(old_version[0].status, TriggerStatus::Active);
        }

        Ok(())
    }
}
