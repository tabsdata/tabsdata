//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use getset::Getters;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::FromRow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use td_common::name::{is_valid_name, is_valid_name_with_dot, name_max_len};
use td_common::uri::{TdUri, TdUriError, TdUriNameId, ToUriString, Versions};
use td_database::sql::create_bindings_literal;
use td_error::td_error;
use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dto::DatasetWrite;
use td_objects::dlo::{CollectionId, CollectionName, DatasetId};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

#[derive(Debug, Clone, PartialEq, Getters)]
#[getset(get = "pub")]
pub struct Dependency {
    collection: String,
    table: String,
    versions: Versions,
}

#[derive(Debug, Clone, PartialEq, Getters)]
#[getset(get = "pub")]
pub struct Trigger {
    collection: String,
    table: String,
}

#[derive(Debug, Clone, FromRow, Getters)]
#[getset(get = "pub")]
pub struct Table {
    name: String,
    collection_id: String,
    collection: String,
    dataset_id: String,
    dataset: String,
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct Relationships {
    tables: Vec<Table>,
    dependencies: Vec<Dependency>,
    triggers: Vec<Trigger>,
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct ResolvedDependency {
    collection: String,
    collection_id: String,
    dataset: String,
    dataset_id: String,
    table: String,
    versions: Versions,
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct ResolvedTrigger {
    collection: String,
    collection_id: String,
    dataset: String,
    dataset_id: String,
}

#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct ResolvedRelationships {
    dependencies: Vec<ResolvedDependency>,
    triggers: Vec<ResolvedTrigger>,
}

trait Resolver<T> {
    fn full_name(&self) -> String;

    fn resolve(&self, table: &Table) -> T;
}

impl Dependency {
    pub fn to_trigger(&self) -> Trigger {
        Trigger {
            collection: self.collection.clone(),
            table: self.table.clone(),
        }
    }
}

impl Resolver<ResolvedDependency> for Dependency {
    fn full_name(&self) -> String {
        format!("{}/{}", self.collection(), self.table())
    }

    fn resolve(&self, table: &Table) -> ResolvedDependency {
        ResolvedDependency {
            collection: self.collection.clone(),
            collection_id: table.collection_id.clone(),
            dataset: table.dataset.clone(),
            dataset_id: table.dataset_id.clone(),
            table: self.table.clone(),
            versions: self.versions.clone(),
        }
    }
}

impl Resolver<ResolvedTrigger> for Trigger {
    fn full_name(&self) -> String {
        format!("{}/{}", self.collection(), self.table())
    }

    fn resolve(&self, table: &Table) -> ResolvedTrigger {
        ResolvedTrigger {
            collection: self.collection.clone(),
            collection_id: table.collection_id.clone(),
            dataset: table.dataset.clone(),
            dataset_id: table.dataset_id.clone(),
        }
    }
}

impl Relationships {
    pub fn collections(&self) -> HashSet<String> {
        self.dependencies()
            .iter()
            .map(|d| d.collection().to_string())
            .chain(self.triggers().iter().map(|t| t.collection().to_string()))
            .collect()
    }

    fn map<T: Resolver<R>, R>(
        tables: &[Table],
        list: &Vec<T>,
        table_map: &HashMap<String, &Table>,
    ) -> Result<Vec<R>, TdError> {
        // merge tables defined by the function and tables already in the system.
        let mut table_map = table_map.clone();
        table_map.extend(
            tables
                .iter()
                .map(|t| (t.full_name(), t))
                .collect::<HashMap<_, _>>(),
        );
        let mut tables_not_found = Vec::new();
        let mut res = Vec::new();
        for item in list {
            if let Some(table) = table_map.get(&item.full_name()) {
                res.push(item.resolve(table));
            } else {
                tables_not_found.push(item.full_name());
            }
        }
        if !tables_not_found.is_empty() {
            Err(DatasetError::TablesNotFound(
                tables_not_found.into_iter().join(", "),
            ))?;
        }
        Ok(res)
    }

    pub fn resolve(
        &self,
        table_map: &HashMap<String, &Table>,
    ) -> Result<ResolvedRelationships, TdError> {
        Ok(ResolvedRelationships {
            dependencies: Self::map(self.tables(), self.dependencies(), table_map)?,
            triggers: Self::map(&[], self.triggers(), table_map)?,
        })
    }
}

impl Table {
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.collection, self.name)
    }
}

impl ResolvedRelationships {
    pub fn dependency_tables(&self) -> Vec<(&str, &str)> {
        self.dependencies
            .iter()
            .map(|d| (d.collection_id().as_str(), d.table().as_str()))
            .dedup()
            .collect()
    }

    pub fn dependency_uris(&self) -> Vec<TdUriNameId> {
        self.dependencies
            .iter()
            .map(|d| {
                TdUriNameId::from(
                    &TdUri::table_uri(d.collection(), d.dataset(), d.table(), d.versions()),
                    d.collection_id(),
                    d.dataset_id(),
                )
            })
            .collect()
    }

    pub fn trigger_uris(&self) -> Vec<TdUriNameId> {
        self.triggers
            .iter()
            .map(|t| {
                TdUriNameId::from(
                    &TdUri::trigger_uri(t.collection(), t.dataset()),
                    t.collection_id(),
                    t.dataset_id(),
                )
            })
            .collect()
    }
}

/// dependencies: T, DS/T, DS/T@Vs
/// triggers: T, DS/T

#[td_error]
pub enum DependencyError {
    #[error("Name cannot be empty")]
    CannotBeEmpty = 0,
    #[error("Name '{0}' cannot be longer than 100 characters")]
    TooLong(String) = 1,
    #[error(
        "Name '{0}' must start with a [a-zA-Z_] character followed by [a-zA-Z0-9_-] characters"
    )]
    InvalidName(String) = 2,
    #[error(
        "Name '{0}' must start with a [.a-zA-Z_] character followed by [.a-zA-Z0-9_-] characters"
    )]
    InvalidNameWithDot(String) = 3,
    #[error(
        "Invalid dependency syntax '{0}'. It must be <table>, <table>@<versions>, <collection>/<table> or <collection>/<table>@<versions>"
    )]
    InvalidDependency(String) = 4,
    #[error("Invalid trigger syntax '{0}'.It must be <table> or <collection>/<table>")]
    InvalidTrigger(String) = 5,
    #[error("A trigger '{0}' cannot have versions")]
    TriggerCannotHaveVersions(String) = 6,
    #[error("{0}")]
    InvalidVersions(#[source] TdUriError) = 7,
}

fn parse_name(name: &str) -> Result<String, TdError> {
    if name.is_empty() {
        Err(DependencyError::CannotBeEmpty)?;
    }
    if name.len() > name_max_len() {
        Err(DependencyError::TooLong(name.to_string()))?;
    }
    if !is_valid_name(name) {
        Err(DependencyError::InvalidName(name.to_string()))?;
    }
    Ok(name.to_string())
}

fn parse_name_with_dot(name: &str) -> Result<String, TdError> {
    if name.is_empty() {
        Err(DependencyError::CannotBeEmpty)?;
    }
    if name.len() > name_max_len() {
        Err(DependencyError::TooLong(name.to_string()))?;
    }
    if !is_valid_name_with_dot(name) {
        Err(DependencyError::InvalidNameWithDot(name.to_string()))?;
    }
    Ok(name.to_string())
}

const TABLE_WITH_VERSIONS_REGEX: &str =
    "^((?<collection>([^/@]+))/)?(?<table>([^/@]+))(@(?<versions>([^/@]+)))?$";

/// parse a dependency from T, DS/T or DS/T@Vs
fn parse_dependency(collection_in_context: &str, dependency: &str) -> Result<Dependency, TdError> {
    lazy_static! {
        static ref DEPENDENCY_REGEX: Regex = Regex::new(TABLE_WITH_VERSIONS_REGEX).unwrap();
    }

    match DEPENDENCY_REGEX.captures(dependency) {
        None => Err(DependencyError::InvalidDependency(dependency.to_string()))?,
        Some(captures) => {
            let collection = captures
                .name("collection")
                .map(|ds| ds.as_str())
                .unwrap_or(collection_in_context);
            let table = captures
                .name("table")
                .map(|t| t.as_str())
                .ok_or_else(|| DependencyError::InvalidDependency(dependency.to_string()))?;
            let versions = captures.name("versions").map(|v| v.as_str());

            let collection = parse_name(collection)?;

            let table = parse_name_with_dot(table)?;

            let versions = match versions {
                None => Versions::None,
                Some(versions) => TdUri::parse_versions(dependency, versions)
                    .map_err(DependencyError::InvalidVersions)?,
            };
            Ok(Dependency {
                collection,
                table,
                versions,
            })
        }
    }
}

/// parse a trigger from T, DS/T
fn parse_trigger(collection_in_context: &str, trigger: &str) -> Result<Trigger, TdError> {
    lazy_static! {
        static ref TRIGGER_REGEX: Regex = Regex::new(TABLE_WITH_VERSIONS_REGEX).unwrap();
    }

    match TRIGGER_REGEX.captures(trigger) {
        None => Err(DependencyError::InvalidTrigger(trigger.to_string()))?,
        Some(captures) => {
            let collection = captures
                .name("collection")
                .map(|ds| ds.as_str())
                .unwrap_or(collection_in_context);
            let table = captures
                .name("table")
                .map(|t| t.as_str())
                .ok_or_else(|| DependencyError::InvalidTrigger(trigger.to_string()))?;
            let versions = captures.name("versions").map(|v| v.as_str());

            let collection = parse_name(collection)?;

            let table = parse_name_with_dot(table)?;

            match versions {
                None => Versions::None,
                _ => Err(DependencyError::TriggerCannotHaveVersions(
                    trigger.to_string(),
                ))?,
            };
            Ok(Trigger { collection, table })
        }
    }
}

pub async fn extract_relationships(
    Input(collection_name): Input<CollectionName>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_id): Input<DatasetId>,
    Input(dataset): Input<DatasetWrite>,
) -> Result<Relationships, TdError> {
    let collection_name = collection_name.as_str();

    let tables = dataset
        .tables()
        .iter()
        .map(|table| Table {
            name: table.to_string(),
            collection_id: collection_id.to_string(),
            collection: collection_name.to_string(),
            dataset: dataset.name().to_string(),
            dataset_id: dataset_id.to_string(),
        })
        .collect();

    let dependencies = dataset
        .dependencies()
        .iter()
        .map(|dep| parse_dependency(collection_name, dep))
        .collect::<Result<Vec<_>, _>>()?;

    let triggers = if let Some(triggers) = dataset.trigger_by() {
        // function specifies triggers, we use those
        triggers
            .iter()
            .map(|trigger| parse_trigger(collection_name, trigger))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        // function does not specify triggers, then all dependencies are triggers
        let triggers: Vec<_> = dependencies.iter().map(|d| d.to_trigger()).collect();
        // we filter out tables produced by the dataset function and dedup other datasets
        let tables = dataset.tables();
        triggers
            .into_iter()
            .filter(|t| !(t.collection() == collection_name && tables.contains(t.table())))
            .dedup()
            .collect()
    };
    Ok(Relationships {
        tables,
        dependencies,
        triggers,
    })
}

pub async fn get_involved_collections_tables(
    Connection(connection): Connection,
    Input(relationships): Input<Relationships>,
) -> Result<Vec<Table>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let collections = relationships.collections();

    const SELECT_TABLES: &str = r#"
        SELECT
            name,
            collection_id,
            collection,
            dataset_id,
            dataset
        FROM ds_current_tables_with_names
        WHERE collection IN ({})
          AND name IS NOT 'td-initial-values'
    "#;
    let query = SELECT_TABLES.replace("{}", &create_bindings_literal(0, collections.len()));

    let mut query_as = sqlx::query_as(&query);
    for key in collections.iter() {
        query_as = query_as.bind(key);
    }
    let tables = query_as.fetch_all(conn).await.map_err(handle_sql_err)?;
    Ok(tables)
}

pub async fn validate_function_tables(
    Input(collection_name): Input<CollectionName>,
    Input(dataset): Input<DatasetWrite>,
    Input(dataset_id): Input<DatasetId>,
    Input(tables): Input<Vec<Table>>,
) -> Result<(), TdError> {
    let dataset_tables = dataset.tables();
    let tables_in_collection_not_in_dataset = tables
        .iter()
        // keep tables within the same collection
        .filter(|t| {
            t.collection() == collection_name.deref().as_ref()
                && t.dataset_id() != dataset_id.deref().as_ref()
        })
        .map(Table::name)
        .collect::<HashSet<_>>();

    let mut already_existing_tables = Vec::new();
    dataset_tables
        .iter()
        .filter(|table| tables_in_collection_not_in_dataset.contains(table))
        .for_each(|table| {
            already_existing_tables.push(table);
        });
    if !already_existing_tables.is_empty() {
        Err(DatasetError::TablesAlreadyDefinedInCollection(
            already_existing_tables.into_iter().join(", "),
        ))?;
    }
    Ok(())
}

pub async fn resolve_relationships(
    Input(tables): Input<Vec<Table>>,
    Input(relationships): Input<Relationships>,
) -> Result<ResolvedRelationships, TdError> {
    let key_table_map: HashMap<String, &Table> =
        tables.iter().map(|t| (t.full_name(), t)).collect();
    relationships.resolve(&key_table_map)
}

pub async fn convert_dataset_write(
    Input(dataset): Input<DatasetWrite>,
    Input(relationships): Input<ResolvedRelationships>,
) -> Result<DatasetWrite, TdError> {
    let mut dataset = dataset.as_ref().clone();
    dataset.dependencies = relationships
        .dependency_uris()
        .iter()
        .map(|uri| uri.with_names().to_uri_string())
        .collect();
    dataset.trigger_by = Some(
        relationships
            .trigger_uris()
            .iter()
            .map(|uri| uri.with_names().to_uri_string())
            .collect(),
    );
    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use super::{
        Dependency, DependencyError, ResolvedDependency, ResolvedRelationships, ResolvedTrigger,
        Resolver,
    };
    use td_common::id;
    use td_common::uri::Version::Head;
    use td_common::uri::{ToUriString, Version, Versions};
    use td_objects::datasets::dto::DatasetWrite;
    use td_objects::dlo::{CollectionId, CollectionName, DatasetId};
    use td_tower::extractors::Input;

    #[test]
    fn test_dependency_to_trigger() {
        let dependency = Dependency {
            collection: "collection".to_string(),
            table: "table".to_string(),
            versions: Versions::None,
        };
        let trigger = dependency.to_trigger();
        assert_eq!(trigger.collection(), "collection");
        assert_eq!(trigger.table(), "table");
    }

    #[test]
    fn test_dependency_resolver() {
        let table = super::Table {
            name: "table".to_string(),
            collection_id: "collection_id".to_string(),
            collection: "collection".to_string(),
            dataset_id: "dataset_id".to_string(),
            dataset: "dataset".to_string(),
        };
        let dependency = Dependency {
            collection: "collection".to_string(),
            table: "table".to_string(),
            versions: Versions::None,
        };

        assert_eq!(dependency.full_name(), "collection/table");

        let resolved_dependency = dependency.resolve(&table);
        assert_eq!(resolved_dependency.collection(), "collection");
        assert_eq!(resolved_dependency.collection_id(), "collection_id");
        assert_eq!(resolved_dependency.dataset(), "dataset");
        assert_eq!(resolved_dependency.dataset_id(), "dataset_id");
        assert_eq!(resolved_dependency.table(), "table");
        assert_eq!(resolved_dependency.versions(), &Versions::None);
    }

    #[test]
    fn test_trigger_resolver() {
        let table = super::Table {
            name: "table".to_string(),
            collection_id: "collection_id".to_string(),
            collection: "collection".to_string(),
            dataset_id: "dataset_id".to_string(),
            dataset: "dataset".to_string(),
        };
        let trigger = super::Trigger {
            collection: "collection".to_string(),
            table: "table".to_string(),
        };

        assert_eq!(trigger.full_name(), "collection/table");

        let resolved_trigger = trigger.resolve(&table);
        assert_eq!(resolved_trigger.collection(), "collection");
        assert_eq!(resolved_trigger.collection_id(), "collection_id");
        assert_eq!(resolved_trigger.dataset(), "dataset");
        assert_eq!(resolved_trigger.dataset_id(), "dataset_id");
    }

    #[test]
    fn test_relationships() {
        let table0 = super::Table {
            name: "table0".to_string(),
            collection_id: "collection_id0".to_string(),
            collection: "collection0".to_string(),
            dataset_id: "dataset_id0".to_string(),
            dataset: "dataset0".to_string(),
        };
        let dependency = Dependency {
            collection: "collection0".to_string(),
            table: "table0".to_string(),
            versions: Versions::None,
        };
        let table1 = super::Table {
            name: "table1".to_string(),
            collection_id: "collection_id1".to_string(),
            collection: "collection1".to_string(),
            dataset_id: "dataset_id1".to_string(),
            dataset: "dataset1".to_string(),
        };
        let trigger = super::Trigger {
            collection: "collection1".to_string(),
            table: "table1".to_string(),
        };
        let relationships = super::Relationships {
            tables: vec![],
            dependencies: vec![dependency],
            triggers: vec![trigger],
        };

        let key_table_map: std::collections::HashMap<String, &super::Table> =
            vec![&table0, &table1]
                .into_iter()
                .map(|t| (t.full_name(), t))
                .collect();
        let resolved_relationships = relationships.resolve(&key_table_map).unwrap();

        let dependency_uris = resolved_relationships.dependency_uris();
        assert_eq!(dependency_uris.len(), 1);
        assert_eq!(
            dependency_uris[0].with_names().to_uri_string(),
            "td:///collection0/dataset0/table0"
        );
        assert_eq!(
            dependency_uris[0].with_ids().to_uri_string(),
            "td:///collection_id0/dataset_id0/table0"
        );

        let trigger_uris = resolved_relationships.trigger_uris();
        assert_eq!(trigger_uris.len(), 1);
        assert_eq!(
            trigger_uris[0].with_names().to_uri_string(),
            "td:///collection1/dataset1"
        );
        assert_eq!(
            trigger_uris[0].with_ids().to_uri_string(),
            "td:///collection_id1/dataset_id1"
        );
    }

    #[test]
    fn test_table() {
        let table = super::Table {
            name: "table".to_string(),
            collection_id: "collection_id".to_string(),
            collection: "collection".to_string(),
            dataset_id: "dataset_id".to_string(),
            dataset: "dataset".to_string(),
        };
        assert_eq!(table.full_name(), "collection/table");
    }

    #[test]
    pub fn test_resolved_relationships() {
        let table0 = super::Table {
            name: "table0".to_string(),
            collection_id: "collection_id0".to_string(),
            collection: "collection0".to_string(),
            dataset_id: "dataset_id0".to_string(),
            dataset: "dataset0".to_string(),
        };
        let dependency = Dependency {
            collection: "collection0".to_string(),
            table: "table0".to_string(),
            versions: Versions::None,
        };
        let table1 = super::Table {
            name: "table1".to_string(),
            collection_id: "collection_id1".to_string(),
            collection: "collection1".to_string(),
            dataset_id: "dataset_id1".to_string(),
            dataset: "dataset1".to_string(),
        };
        let trigger = super::Trigger {
            collection: "collection1".to_string(),
            table: "table1".to_string(),
        };
        let relationships = super::Relationships {
            tables: vec![], //todo
            dependencies: vec![dependency],
            triggers: vec![trigger],
        };

        let key_table_map: std::collections::HashMap<String, &super::Table> =
            vec![&table0, &table1]
                .into_iter()
                .map(|t| (t.full_name(), t))
                .collect();
        let resolved_relationships = relationships.resolve(&key_table_map).unwrap();

        let dependency_tables = resolved_relationships.dependency_tables();
        assert_eq!(dependency_tables.len(), 1);
        assert_eq!(dependency_tables[0], ("collection_id0", "table0"));

        let dependency_uris = resolved_relationships.dependency_uris();
        assert_eq!(dependency_uris.len(), 1);
        assert_eq!(
            dependency_uris[0].with_names().to_uri_string(),
            "td:///collection0/dataset0/table0"
        );
        assert_eq!(
            dependency_uris[0].with_ids().to_uri_string(),
            "td:///collection_id0/dataset_id0/table0"
        );

        let trigger_uris = resolved_relationships.trigger_uris();
        assert_eq!(trigger_uris.len(), 1);
        assert_eq!(
            trigger_uris[0].with_names().to_uri_string(),
            "td:///collection1/dataset1"
        );
        assert_eq!(
            trigger_uris[0].with_ids().to_uri_string(),
            "td:///collection_id1/dataset_id1"
        );
    }

    // fn get_err<T: Debug, E: Error + 'static>(res: &Result<T, TdError>) -> &E {
    //     res.as_ref().unwrap_err().source().unwrap().downcast_ref::<E>().unwrap()
    // }
    #[test]
    fn test_parse_name() {
        assert!(matches!(
            super::parse_name("").unwrap_err().domain_err(),
            &DependencyError::CannotBeEmpty
        ));
        assert!(matches!(
            super::parse_name(&"a".repeat(101))
                .unwrap_err()
                .domain_err(),
            &DependencyError::TooLong(_)
        ));
        assert!(matches!(
            super::parse_name("!").unwrap_err().domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_name("0").unwrap_err().domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_name("a ").unwrap_err().domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_name(" a").unwrap_err().domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_name("a a").unwrap_err().domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_name("a.").unwrap_err().domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert_eq!(super::parse_name("a").unwrap(), "a");
        assert_eq!(
            super::parse_name(&"a".repeat(100)).unwrap(),
            "a".repeat(100)
        );
        assert_eq!(super::parse_name("a0_-").unwrap(), "a0_-");
    }

    #[test]
    fn test_parse_name_with_dot() {
        assert!(matches!(
            super::parse_name_with_dot("").unwrap_err().domain_err(),
            &DependencyError::CannotBeEmpty
        ));
        assert!(matches!(
            super::parse_name_with_dot(&"a".repeat(101))
                .unwrap_err()
                .domain_err(),
            &DependencyError::TooLong(_)
        ));
        assert!(matches!(
            super::parse_name_with_dot("!").unwrap_err().domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_name_with_dot("0").unwrap_err().domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_name_with_dot("a ").unwrap_err().domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_name_with_dot(" a").unwrap_err().domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_name_with_dot("a a").unwrap_err().domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert_eq!(super::parse_name_with_dot("a").unwrap(), "a");
        assert_eq!(super::parse_name_with_dot("a.").unwrap(), "a.");
        assert_eq!(
            super::parse_name_with_dot(&"a".repeat(100)).unwrap(),
            "a".repeat(100)
        );
        assert_eq!(super::parse_name_with_dot("a0_-").unwrap(), "a0_-");
    }

    #[test]
    fn test_parse_dependency() {
        assert!(matches!(
            super::parse_dependency("collection", "")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidDependency(_)
        ));
        assert!(matches!(
            super::parse_dependency("collection", "t!")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_dependency("collection", "ds!/t")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_dependency("collection", "ds/t!")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_dependency("collection", "t@x")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidVersions(_)
        ));
        assert_eq!(
            super::parse_dependency("collection", "t").unwrap(),
            Dependency {
                collection: "collection".to_string(),
                table: "t".to_string(),
                versions: Versions::None
            }
        );
        assert_eq!(
            super::parse_dependency("collection", "ds/t").unwrap(),
            Dependency {
                collection: "ds".to_string(),
                table: "t".to_string(),
                versions: Versions::None
            }
        );
        assert_eq!(
            super::parse_dependency("collection", "ds/t@HEAD").unwrap(),
            Dependency {
                collection: "ds".to_string(),
                table: "t".to_string(),
                versions: Versions::Single(Version::Head(0))
            }
        );
        assert_eq!(
            super::parse_dependency("collection", "ds/t@HEAD^,HEAD").unwrap(),
            Dependency {
                collection: "ds".to_string(),
                table: "t".to_string(),
                versions: Versions::List(vec![Version::Head(-1), Version::Head(0)])
            }
        );
        assert_eq!(
            super::parse_dependency("collection", "ds/t@HEAD^..HEAD").unwrap(),
            Dependency {
                collection: "ds".to_string(),
                table: "t".to_string(),
                versions: Versions::Range(Version::Head(-1), Version::Head(0))
            }
        );
        let fixed = id::id();
        assert_eq!(
            super::parse_dependency("collection", &format!("ds/t@{}", fixed)).unwrap(),
            Dependency {
                collection: "ds".to_string(),
                table: "t".to_string(),
                versions: Versions::Single(Version::Fixed(fixed))
            }
        );
    }

    #[test]
    fn test_parse_trigger() {
        assert!(matches!(
            super::parse_trigger("collection", "")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidTrigger(_)
        ));
        assert!(matches!(
            super::parse_trigger("collection", "t!")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_trigger("collection", "ds!/t")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidName(_)
        ));
        assert!(matches!(
            super::parse_trigger("collection", "ds/t!")
                .unwrap_err()
                .domain_err(),
            &DependencyError::InvalidNameWithDot(_)
        ));
        assert!(matches!(
            super::parse_trigger("collection", "t@x")
                .unwrap_err()
                .domain_err(),
            &DependencyError::TriggerCannotHaveVersions(_)
        ));
        assert_eq!(
            super::parse_trigger("collection", "t").unwrap(),
            super::Trigger {
                collection: "collection".to_string(),
                table: "t".to_string()
            }
        );
        assert_eq!(
            super::parse_trigger("collection", "ds/t").unwrap(),
            super::Trigger {
                collection: "ds".to_string(),
                table: "t".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_extract_relationships_none() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec![],
            dependencies: vec![],
            trigger_by: None,
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // no trigger, no dependencies
        assert!(relationships.dependencies().is_empty());
        assert!(relationships.triggers().is_empty());
    }

    #[tokio::test]
    async fn test_extract_relationships_external_deps() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec![],
            dependencies: vec!["ds0/t0".to_string()],
            trigger_by: None,
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // triggers equals dependencies
        println!("{:#?}", relationships);
    }

    #[tokio::test]
    async fn test_extract_relationships_external_deps_dup() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec![],
            dependencies: vec!["ds0/t0".to_string(), "ds0/t0".to_string()],
            trigger_by: None,
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // trigger equals dedup-ed dependency tables
        println!("{:#?}", relationships);
    }

    #[tokio::test]
    async fn test_extract_relationships_internal_deps() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec!["t0".to_string()],
            trigger_by: None,
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // all internal dependencies, no triggers
        println!("{:#?}", relationships);
    }

    #[tokio::test]
    async fn test_extract_relationships_internal_external_deps() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec!["t0".to_string(), "t1".to_string()],
            trigger_by: None,
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // triggers are only external dependencies
        println!("{:#?}", relationships);
    }

    #[tokio::test]
    async fn test_extract_relationships_explicit_trigger() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec!["t1".to_string()],
            trigger_by: Some(vec!["t2".to_string()]),
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // triggers are not inferred from dependencies
        println!("{:#?}", relationships);
    }

    #[tokio::test]
    async fn test_extract_relationships_explicit_no_trigger() {
        let collection_name = CollectionName("collection".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec!["t1".to_string()],
            trigger_by: Some(vec![]),
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // explicit no triggers
        println!("{:#?}", relationships);
    }

    #[tokio::test]
    async fn test_extract_relationships_collections() {
        let collection_name = CollectionName("ds0".to_string());
        let collection_id = CollectionId("collection_i".to_string());
        let dataset_id = DatasetId("dataset_i".to_string());
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec!["t0".to_string(), "ds1/t0".to_string()],
            trigger_by: Some(vec!["t1".to_string(), "ds2/t1".to_string()]),
            function_snippet: None,
        };
        let relationships = super::extract_relationships(
            Input::new(collection_name),
            Input::new(collection_id),
            Input::new(dataset_id),
            Input::new(dataset),
        )
        .await
        .unwrap();
        // all collections involved
        println!("{:#?}", relationships.collections());
    }

    #[tokio::test]
    async fn test_validate_function_tables_ok() {
        let collection_name = CollectionName("ds0".to_string());
        let collection_name = Input::new(collection_name);
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string()],
            dependencies: vec!["t0".to_string(), "ds1/t0".to_string()],
            trigger_by: Some(vec!["t1".to_string()]),
            function_snippet: None,
        };
        let dataset = Input::new(dataset);
        let dataset_id = DatasetId("d0i".to_string());
        let dataset_id = Input::new(dataset_id);
        let tables = vec![
            super::Table {
                name: "t0".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d0i".to_string(),
                dataset: "dataset".to_string(),
            },
            super::Table {
                name: "t0".to_string(),
                collection_id: "ds1i".to_string(),
                collection: "ds1".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "dataset".to_string(),
            },
            super::Table {
                name: "t1".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "d1".to_string(),
            },
            super::Table {
                name: "t2".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "d1".to_string(),
            },
        ];
        let tables = Input::new(tables);
        let res =
            super::validate_function_tables(collection_name, dataset, dataset_id, tables).await;
        println!("{:#?}", res);
    }

    #[tokio::test]
    async fn test_validate_function_tables_err() {
        let collection_name = CollectionName("ds0".to_string());
        let collection_name = Input::new(collection_name);
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string(), "t2".to_string()],
            dependencies: vec!["t0".to_string(), "ds1/t0".to_string()],
            trigger_by: Some(vec!["t1".to_string()]),
            function_snippet: None,
        };
        let dataset = Input::new(dataset);
        let dataset_id = DatasetId("d0i".to_string());
        let dataset_id = Input::new(dataset_id);
        let tables = vec![
            super::Table {
                name: "t0".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d0i".to_string(),
                dataset: "dataset".to_string(),
            },
            super::Table {
                name: "t2".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "dataset1".to_string(),
            },
            super::Table {
                name: "t0".to_string(),
                collection_id: "ds1i".to_string(),
                collection: "ds1".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "dataset".to_string(),
            },
            super::Table {
                name: "t1".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "d1".to_string(),
            },
            super::Table {
                name: "t2".to_string(),
                collection_id: "ds0i".to_string(),
                collection: "ds0".to_string(),
                dataset_id: "d1i".to_string(),
                dataset: "d1".to_string(),
            },
        ];
        let tables = Input::new(tables);
        let res =
            super::validate_function_tables(collection_name, dataset, dataset_id, tables).await;
        println!("{:#?}", res);
    }

    #[tokio::test]
    async fn test_convert_dataset_write() {
        let dataset = DatasetWrite {
            name: "dataset".to_string(),
            description: "".to_string(),
            data_location: None,
            bundle_hash: "".to_string(),
            tables: vec!["t0".to_string(), "t2".to_string()],
            dependencies: vec!["t0".to_string()],
            trigger_by: Some(vec!["ds1/t1".to_string()]),
            function_snippet: None,
        };
        let relationships = ResolvedRelationships {
            dependencies: vec![ResolvedDependency {
                collection: "ds0".to_string(),
                table: "t0".to_string(),
                versions: Versions::Single(Head(-1)),
                collection_id: "ds0i".to_string(),
                dataset: "dataset".to_string(),
                dataset_id: "d0i".to_string(),
            }],
            triggers: vec![ResolvedTrigger {
                collection: "ds1".to_string(),
                collection_id: "ds1i".to_string(),
                dataset: "dataset1".to_string(),
                dataset_id: "d1i".to_string(),
            }],
        };

        let dataset = Input::new(dataset);
        let relationships = Input::new(relationships);
        let res = super::convert_dataset_write(dataset, relationships).await;
        println!("{:#?}", res);
    }
}
