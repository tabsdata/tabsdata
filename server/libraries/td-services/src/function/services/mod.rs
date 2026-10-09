//
// Copyright 2025 Tabsdata Inc.
//

pub(crate) mod delete;
pub mod download_bundle;
pub(crate) mod enable;
pub(crate) mod history;
pub(crate) mod list;
pub(crate) mod list_by_collection;
pub(crate) mod list_patches;
pub(crate) mod patch;
pub(crate) mod read;
pub(crate) mod register;
pub(crate) mod update;
pub(crate) mod upload;

#[cfg(test)]
pub(crate) mod tests {
    use chrono::DateTime;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::dependency::DependencyDBWithNames;
    use td_objects::dxo::function::{Function, FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::dxo::table::{TableDB, TableDBWithNames};
    use td_objects::dxo::trigger::TriggerDBWithNames;
    use td_objects::system::{
        SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES, SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES,
    };
    use td_objects::types::basic::{
        AtTime, CollectionIdName, Enabled, FunctionStatus, TableIdName, TableName, TableStatus,
        TriggerStatus, UserId,
    };
    use td_objects::types::composed::{TableDependency, TableTrigger, TableTriggerDto};
    use td_objects::types::table_ref::TableRef;
    use td_queries::Queries;
    pub async fn assert_register(
        db: &TdPool,
        user_id: &UserId,
        collection: &ActiveCollectionDB,
        create: &FunctionRegister,
        response: &Function,
    ) -> Result<(), TdError> {
        let req_tables = create.tables.as_deref().unwrap_or(&[]);
        let req_dependencies = create.dependencies.as_deref().unwrap_or(&[]);
        // Mirrors build_trigger_versions' default-trigger derivation.
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

        assert_eq!(response.collection_id, collection.id);
        assert_eq!(response.name, create.name);
        assert_eq!(response.description, create.description);
        assert_eq!(response.status, FunctionStatus::Active);
        assert_eq!(response.decorator, create.decorator);
        assert_eq!(response.bundle_id, create.bundle_id);
        assert_eq!(*response.snippet, *create.snippet);
        assert_eq!(response.defined_by_id, *user_id);
        assert_eq!(response.collection, collection.name);
        assert_eq!(response.enabled, Enabled::TRUE);

        let function_version_id = response.id.clone();
        let function_id = response.function_id;

        let functions: Vec<FunctionDB> = db
            .timeline()
            .fetch_all::<FunctionDB>(&function_version_id)
            .await?;
        assert_eq!(functions.len(), 1);
        let function = &functions[0];
        assert_eq!(function.base.collection_id, response.collection_id);
        assert_eq!(function.base.name, response.name);
        assert_eq!(function.base.function_id, response.function_id);
        assert_eq!(function.base.bundle_id, response.bundle_id);
        assert_eq!(function.base.snippet, response.snippet);
        assert_eq!(function.base.decorator, response.decorator);
        assert_eq!(function.base.status, FunctionStatus::Active);

        let table_versions: Vec<TableDB> = db
            .scd2()
            .fetch_all(and!(&function_id, TableDB::active()))
            .await?;
        assert_eq!(
            table_versions.len(),
            req_tables.len() + SYSTEM_OUTPUT_TABLE_NAMES_PREFIXES.len()
        );
        for table in req_tables {
            let found = table_versions
                .iter()
                .find(|t| **t.base.name == **table)
                .expect("table version not found");
            assert_eq!(found.base.collection_id, function.base.collection_id);
            assert_eq!(**found.base.name, **table);
            // Defined by this version or by an earlier one it left unchanged.
            assert_eq!(found.base.function_id, function.base.function_id);
            assert!(found.base.function_param_pos.is_some());
            assert!(found.interval.valid_from <= function.interval.valid_from);
            assert_eq!(found.base.defined_by_id, function.base.defined_by_id);
            assert_eq!(found.base.status, TableStatus::Active);
        }

        let dependency_versions: Vec<DependencyDBWithNames> =
            db.scd2().fetch_all(&function_id).await?;
        assert_eq!(
            dependency_versions.len(),
            req_dependencies.len() + SYSTEM_INPUT_TABLE_DEPENDENCY_PREFIXES.len()
        );
        for dependency in req_dependencies {
            let dependency = &TableDependency::try_from(dependency)?;
            let dependency_table = db
                .scd2()
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        dependency
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(dependency.table.clone())
                ))
                .await?;
            let found = dependency_versions
                .iter()
                .find(|d| d.base.base.table_id == dependency_table.base.base.table_id)
                .expect("dependency version not found");
            assert_eq!(found.base.base.collection_id, function.base.collection_id);
            assert_eq!(found.base.base.function_id, function.base.function_id);
            assert_eq!(
                *found.table_collection,
                *dependency.collection.as_ref().unwrap_or(&collection.name)
            );
            assert_eq!(*found.base.base.table_versions, dependency.versions);
            assert!(found.base.interval.valid_from <= function.interval.valid_from);
            assert_eq!(found.base.base.defined_by_id, function.base.defined_by_id);
        }

        let trigger_versions: Vec<TriggerDBWithNames> = db
            .scd2()
            .fetch_all(and!(&function_id, TriggerDBWithNames::active()))
            .await?;
        assert_eq!(trigger_versions.len(), req_triggers.len());
        for trigger in req_triggers {
            let trigger = TableTrigger::try_from(trigger)?;
            let trigger_table = db
                .scd2()
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        trigger
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(trigger.table.clone())
                ))
                .await?;
            let found = trigger_versions
                .iter()
                .find(|d| d.base.base.trigger_by_table_id == trigger_table.base.base.table_id)
                .expect("trigger version not found");
            assert_eq!(found.base.base.collection_id, function.base.collection_id);
            assert_eq!(found.base.base.function_id, function.base.function_id);
            assert_eq!(
                *found.trigger_by_collection,
                *trigger.collection.as_ref().unwrap_or(&collection.name)
            );
            assert_eq!(found.base.base.status, TriggerStatus::Active);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn assert_update(
        db: &TdPool,
        user_id: &UserId,
        collection: &ActiveCollectionDB,
        create: &FunctionRegister,
        created_function: &FunctionDB,
        update: &FunctionUpdate,
        response: &Function,
    ) -> Result<(), TdError> {
        let functions: Vec<FunctionDB> = db
            .timeline()
            .fetch_all::<FunctionDB>(&created_function.base.function_id)
            .await?;
        assert_eq!(functions.len(), 2);

        let functions: Vec<FunctionDB> = db
            .timeline()
            .fetch_all::<FunctionDB>(&created_function.base.id)
            .await?;
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].base, created_function.base);
        assert!(!functions[0].interval.valid_to.is_infinity());

        match &update.snippet {
            Some(snippet) => assert_eq!(*response.snippet, **snippet),
            None => assert_eq!(response.snippet, created_function.base.snippet),
        }

        // A table the update keeps unchanged is not re-versioned: its open
        // version still belongs to the version that defined it. A dropped
        // table closes that version and freezes.
        for table_dto in create.tables.as_deref().unwrap_or(&[]) {
            let table = &TableName::try_from(table_dto)?;
            let versions = db
                .timeline()
                .fetch_all::<TableDB>(and!(collection.id, table))
                .await?;
            let created = versions
                .iter()
                .find(|v| {
                    v.base.function_version_id == created_function.base.id
                        && v.base.status == TableStatus::Active
                })
                .expect("the created version of the table");
            if update.tables.as_deref().unwrap_or(&[]).contains(table_dto) {
                assert_eq!(versions.len(), 1);
                assert!(created.interval.valid_to.is_infinity());
            } else {
                assert_eq!(versions.len(), 2);
                assert!(!created.interval.valid_to.is_infinity());
                let frozen = versions
                    .iter()
                    .find(|v| v.base.status == TableStatus::Frozen)
                    .expect("the frozen successor");
                assert!(frozen.interval.valid_to.is_infinity());
                assert_eq!(frozen.base.table_id, created.base.table_id);
            }
        }

        let before_at_time = AtTime::from(
            DateTime::from_timestamp_millis(response.defined_on.timestamp_millis() - 1).unwrap(),
        );

        for dependency_dto in create.dependencies.as_deref().unwrap_or(&[]) {
            let dependency = &TableDependency::try_from(dependency_dto)?;

            let dependency_table: TableDBWithNames = db
                .scd2()
                .as_of(&before_at_time)
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        dependency
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(dependency.table.clone())
                ))
                .await?;
            let old_version = db
                .scd2()
                .as_of(&before_at_time)
                .fetch_all::<DependencyDBWithNames>(and!(
                    dependency_table.base.base.table_id,
                    created_function.base.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);

            let dependency_table = db
                .scd2()
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        dependency
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(dependency.table.clone())
                ))
                .await?;
            let new_version = db
                .scd2()
                .fetch_all::<DependencyDBWithNames>(and!(
                    dependency_table.base.base.table_id,
                    response.function_id
                ))
                .await?;
            if update
                .dependencies
                .as_deref()
                .unwrap_or(&[])
                .contains(dependency_dto)
            {
                assert_eq!(new_version.len(), 1);
                assert_eq!(
                    old_version[0].base.base.dependency_id,
                    new_version[0].base.base.dependency_id
                );
            } else {
                assert!(new_version.is_empty());
            }
        }

        for trigger_dto in create.triggers.as_deref().unwrap_or(&[]) {
            let trigger = TableTrigger::try_from(trigger_dto)?;
            let trigger_table: TableDBWithNames = db
                .scd2()
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        trigger
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(trigger.table.clone())
                ))
                .await?;
            let old_version: Vec<TriggerDBWithNames> = db
                .scd2()
                .as_of(&before_at_time)
                .fetch_all::<TriggerDBWithNames>(and!(
                    trigger_table.base.base.table_id,
                    created_function.base.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);

            let new_version = db
                .scd2()
                .fetch_all::<TriggerDBWithNames>(and!(
                    trigger_table.base.base.table_id,
                    response.function_id
                ))
                .await?;
            if update
                .triggers
                .as_deref()
                .unwrap_or(&[])
                .contains(trigger_dto)
            {
                assert_eq!(new_version.len(), 1);
                assert_eq!(new_version[0].base.base.status, TriggerStatus::Active);
                assert_eq!(
                    old_version[0].base.base.trigger_id,
                    new_version[0].base.base.trigger_id
                );
            } else {
                assert!(new_version.is_empty());
            }
        }

        // The new version must look exactly as if it had just been registered.
        let create = create.clone();
        let update = update.clone();
        let update_as_register = FunctionRegister {
            child_functions: None,
            name: update.name.unwrap_or(create.name),
            description: update.description.unwrap_or(create.description),
            bundle_id: update.bundle_id.unwrap_or(create.bundle_id),
            snippet: update.snippet.unwrap_or(create.snippet),
            decorator: update.decorator.unwrap_or(create.decorator),
            connector: update.connector.unwrap_or(create.connector),
            connection: update.connection.unwrap_or(create.connection),
            dependencies: update.dependencies,
            triggers: update.triggers,
            tables: update.tables,
            table_types: update.table_types,
            parent_tables: None,
            runtime_values: update.runtime_values.unwrap_or(create.runtime_values),
            reuse_frozen_tables: update.reuse_frozen_tables,
            enabled: update.enabled,
            workers: None,
            base_path: update.base_path.or(create.base_path),
        };
        assert_register(db, user_id, collection, &update_as_register, response).await?;
        Ok(())
    }

    pub async fn assert_delete(
        db: &TdPool,
        _user_id: &UserId,
        collection: &ActiveCollectionDB,
        create: &FunctionRegister,
        created_function: &FunctionDB,
    ) -> Result<(), TdError> {
        let live: Option<FunctionDB> = db
            .scd2()
            .fetch_optional::<FunctionDB>(&created_function.base.function_id)
            .await?;
        assert!(live.is_none());

        let functions: Vec<FunctionDB> = db
            .timeline()
            .fetch_all::<FunctionDB>(&created_function.base.id)
            .await?;
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].base, created_function.base);
        assert!(!functions[0].interval.valid_to.is_infinity());

        let created_tables: Vec<TableDB> = db
            .timeline()
            .fetch_all::<TableDB>(and!(&created_function.base.id, TableDB::active()))
            .await?;
        assert!(
            created_tables
                .iter()
                .all(|t| !t.interval.valid_to.is_infinity())
        );
        let active: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(and!(&created_function.base.id, TableDB::active()))
            .await?;
        assert!(active.is_empty());
        let current: Vec<TableDB> = db
            .scd2()
            .fetch_all::<TableDB>(&created_function.base.id)
            .await?;
        let user_tables = created_tables
            .iter()
            .filter(|t| !t.base.name.is_private())
            .count();
        assert_eq!(current.len(), user_tables);
        assert!(current.iter().all(|t| t.base.status == TableStatus::Frozen));
        assert!(current.iter().all(|t| !t.base.name.is_private()));

        for table_dto in create.tables.as_deref().unwrap_or(&[]) {
            let table = &TableName::try_from(table_dto)?;
            let versions = db
                .timeline()
                .fetch_all::<TableDB>(and!(collection.id, table, &created_function.base.id))
                .await?;
            assert_eq!(versions.len(), 2);
            let active = versions
                .iter()
                .find(|v| v.base.status == TableStatus::Active);
            let frozen = versions
                .iter()
                .find(|v| v.base.status == TableStatus::Frozen);
            assert!(
                !active
                    .expect("closed active version")
                    .interval
                    .valid_to
                    .is_infinity()
            );
            assert!(
                frozen
                    .expect("frozen successor")
                    .interval
                    .valid_to
                    .is_infinity()
            );
        }

        for dependency in create.dependencies.as_deref().unwrap_or(&[]) {
            let dependency = &TableDependency::try_from(dependency)?;
            let table_dependency = db
                .scd2()
                .as_of(&AtTime::from(created_function.interval.valid_from))
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        dependency
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(dependency.table.clone())
                ))
                .await?;
            let old_version = db
                .scd2()
                .as_of(&AtTime::from(created_function.interval.valid_from))
                .fetch_all::<DependencyDBWithNames>(and!(
                    table_dependency.base.base.table_id,
                    created_function.base.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);
        }

        for trigger_dto in create.triggers.as_deref().unwrap_or(&[]) {
            let trigger = &TableTrigger::try_from(trigger_dto)?;
            let table_trigger = db
                .scd2()
                .as_of(&AtTime::from(created_function.interval.valid_from))
                .fetch_one::<TableDBWithNames>(and!(
                    &CollectionIdName::Name(
                        trigger
                            .collection
                            .clone()
                            .unwrap_or(collection.name.clone())
                    ),
                    &TableIdName::Name(trigger.table.clone())
                ))
                .await?;
            let old_version = db
                .scd2()
                .as_of(&AtTime::from(created_function.interval.valid_from))
                .fetch_all::<TriggerDBWithNames>(and!(
                    table_trigger.base.base.table_id,
                    created_function.base.function_id
                ))
                .await?;
            assert_eq!(old_version.len(), 1);
            assert_eq!(old_version[0].base.base.status, TriggerStatus::Active);
        }

        Ok(())
    }
}
