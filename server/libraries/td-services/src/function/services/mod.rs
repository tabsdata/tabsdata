//
// Copyright 2025 Tabs Data Inc.
//

pub mod read;
pub mod read_version;
pub mod register;
pub mod update;

#[cfg(test)]
pub(crate) mod tests {
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::crudl::handle_sql_err;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{
        DependencyStatus, Frozen, FunctionStatus, TableStatus, TriggerStatus, UserId,
    };
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::dependency::{DependencyDBWithNames, DependencyVersionDBWithNames};
    use td_objects::types::function::{
        FunctionCreate, FunctionDB, FunctionUpdate, FunctionVersion, FunctionVersionDB,
    };
    use td_objects::types::table::{TableDB, TableVersionDB};
    use td_objects::types::trigger::{TriggerDBWithNames, TriggerVersionDBWithNames};

    pub async fn assert_register(
        db: &DbPool,
        user_id: &UserId,
        collection: &CollectionDB,
        create: &FunctionCreate,
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
        assert_eq!(*response.status(), FunctionStatus::active());
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
        assert_eq!(function_version.defined_on(), function.created_on());
        assert_eq!(function_version.defined_by_id(), function.created_by_id());
        assert_eq!(*function_version.status(), FunctionStatus::active());

        // Assert table versions were created (query by active to filter deleted tables in updates)
        let table_versions: Vec<TableVersionDB> = queries
            .select_by::<TableVersionDB>(&(function_version_id, &TableStatus::active()))?
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
                &DependencyStatus::active(),
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
            assert_eq!(*found.status(), DependencyStatus::active());
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
            .select_by::<TriggerVersionDBWithNames>(&(
                function_version_id,
                &TriggerStatus::active(),
            ))?
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
            assert_eq!(*found.status(), TriggerStatus::active());
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
        create: &FunctionCreate,
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
            assert_eq!(*old_version[0].status(), TableStatus::active());

            // And a new one, which will be active if still present, or else frozen
            let new_version: Vec<TableVersionDB> = queries
                .select_by::<TableVersionDB>(&(collection.id(), table, response.id()))?
                .build_query_as()
                .fetch_all(db)
                .await
                .map_err(handle_sql_err)?;
            assert_eq!(new_version.len(), 1);
            let new_status = if update.tables().as_deref().unwrap_or(&[]).contains(table) {
                TableStatus::active()
            } else {
                TableStatus::frozen()
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
            assert_eq!(*old_version[0].status(), DependencyStatus::active());

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
                DependencyStatus::active()
            } else {
                DependencyStatus::deleted()
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
            assert_eq!(*old_version[0].status(), TriggerStatus::active());

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
                TriggerStatus::active()
            } else {
                TriggerStatus::deleted()
            };
            assert_eq!(*new_version[0].status(), new_status);

            // Both with the same trigger_id
            assert_eq!(old_version[0].trigger_id(), new_version[0].trigger_id());
        }

        // And finally, new version should be the exact same as if it just got registered.
        assert_register(db, user_id, collection, update, response).await?;
        Ok(())
    }
}
