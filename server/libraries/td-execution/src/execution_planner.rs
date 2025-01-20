//
// Copyright 2024 Tabs Data Inc.
//

use crate::dataset::{Dataset, DatasetWithUris, RelativeVersions, ResolvedVersion};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use td_common::dataset::{DatasetRef, VersionRef};
use td_common::index;
use td_common::index_map::IndexMap;

index!(DatasetIndex);
index!(VersionIndex);

/// The `ExecutionPlanner` struct represents a plan for executing datasets with specific versions and requirements.
/// It allows to easily transform versioning without changing the base structure and relations of the Datasets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlanner<D, V>
where
    D: DatasetRef,
    V: VersionRef,
{
    /// Index with its Dataset.
    datasets: IndexMap<DatasetIndex, D>,
    /// Index with its Version, which consists in (Dataset, Version).
    versions: IndexMap<VersionIndex, (DatasetIndex, V)>,
    /// Index of the versions that are manually triggered.
    manual_trigger: VersionIndex,
    /// Index of the versions that are dependency triggered.
    dependency_triggers: Vec<VersionIndex>,
    /// Version Index to Version Index of version requirement relation.
    data_requirements: Vec<(VersionIndex, VersionIndex)>,
    /// Version Index to Version Index of trigger requirement relation.
    trigger_requirements: Vec<(VersionIndex, VersionIndex)>,
}

pub type ExecutionTemplate = ExecutionPlanner<Dataset, RelativeVersions>;
pub type ExecutionPlan = ExecutionPlanner<Dataset, ResolvedVersion>;
pub type ExecutionPlanWithNames = ExecutionPlanner<DatasetWithUris, ResolvedVersion>;

impl ExecutionTemplate {
    /// Creates a new, empty `ExecutionPlanner`.
    pub fn with_trigger(manual_trigger: &Dataset) -> Self {
        let mut datasets = IndexMap::new();
        let mut versions = IndexMap::new();
        let manual_trigger_dataset = datasets.insert_if_absent(manual_trigger.clone());
        let manual_trigger_version =
            versions.insert_if_absent((manual_trigger_dataset, RelativeVersions::default()));

        Self {
            datasets,
            versions,
            manual_trigger: manual_trigger_version,
            dependency_triggers: Vec::new(),
            data_requirements: Vec::new(),
            trigger_requirements: Vec::new(),
        }
    }

    /// Adds a dataset if absent and a trigger to the execution plan.
    pub fn add_dependency_trigger(&mut self, dataset: &Dataset) {
        // Add dataset.
        let dataset_index = self.datasets.insert_if_absent(dataset.clone());

        // Add generated version.
        let version_index = self
            .versions
            .insert_if_absent((dataset_index, RelativeVersions::default()));

        // Add dependency triggered version.
        self.dependency_triggers.push(version_index);
    }

    /// Adds a dataset and its version if absent, and the version requirement to the execution plan.
    pub fn add_data_requirement(
        &mut self,
        dataset: &Dataset,
        requires: &Dataset,
        version: RelativeVersions,
    ) {
        let (target_index, source_index) = self.create_requirement(dataset, requires, version);
        self.data_requirements.push((target_index, source_index));
    }

    /// Adds a dataset and its version if absent, and the version requirement to the execution plan.
    pub fn add_trigger_requirement(&mut self, dataset: &Dataset, requires: &Dataset) {
        let (target_index, source_index) =
            self.create_requirement(dataset, requires, RelativeVersions::default());
        self.trigger_requirements.push((target_index, source_index));
    }

    /// Adds a dataset and its version if absent, and the version requirement to the execution plan.
    fn create_requirement(
        &mut self,
        dataset: &Dataset,
        requires: &Dataset,
        version: RelativeVersions,
    ) -> (VersionIndex, VersionIndex) {
        // Add created version with created dataset.
        let index = self.datasets.insert_if_absent(dataset.clone());

        // Target is always planned version.
        let target_version_index = self
            .versions
            .insert_if_absent((index, RelativeVersions::default()));

        // Add required version for required dataset.
        let index = self.datasets.insert_if_absent(requires.clone());
        let source_version_index = self.versions.insert_if_absent((index, version));

        (target_version_index, source_version_index)
    }
}

impl<D, V> ExecutionPlanner<D, V>
where
    D: DatasetRef,
    V: VersionRef,
{
    /// Transforms the execution plan by applying a transformation function to each dataset.
    pub async fn named<'a, DD, F, E, Fut>(
        &'a self,
        transform: F,
    ) -> Result<ExecutionPlanner<DD, V>, E>
    where
        DD: DatasetRef,
        F: Fn(&'a D) -> Fut + Clone + 'a,
        Fut: Future<Output = Result<DD, E>> + Sized,
        V: 'a,
    {
        let futures: Vec<_> = self
            .datasets
            .iter()
            .map(|(di, d)| {
                let transform = transform.clone();
                async move {
                    let new_dataset = transform(d).await?;
                    Ok::<_, E>((*di, new_dataset))
                }
            })
            .collect();

        let new_datasets: IndexMap<_, _> = futures::future::try_join_all(futures)
            .await?
            .into_iter()
            .collect();

        Ok(ExecutionPlanner {
            datasets: new_datasets,
            versions: self.versions.clone(),
            manual_trigger: self.manual_trigger,
            dependency_triggers: self.dependency_triggers.clone(),
            data_requirements: self.data_requirements.clone(),
            trigger_requirements: self.trigger_requirements.clone(),
        })
    }

    /// Transforms the execution plan by applying a transformation function to each version.
    pub async fn versioned<'a, VV, F, E, Fut>(
        &'a self,
        transform: F,
    ) -> Result<ExecutionPlanner<D, VV>, E>
    where
        VV: VersionRef,
        F: Fn(&'a D, &'a V) -> Fut + Clone + 'a,
        Fut: Future<Output = Result<VV, E>> + Sized,
        V: 'a,
    {
        let futures: Vec<_> = self
            .versions
            .iter()
            .map(|(vi, (di, v))| {
                let transform = transform.clone();
                let dataset = self.datasets.get(di).unwrap();
                async move {
                    let new_version = transform(dataset, v).await?;
                    Ok::<_, E>((*vi, (*di, new_version)))
                }
            })
            .collect();

        let new_versions: IndexMap<_, _> = futures::future::try_join_all(futures)
            .await?
            .into_iter()
            .collect();

        Ok(ExecutionPlanner {
            datasets: self.datasets.clone(),
            versions: new_versions,
            manual_trigger: self.manual_trigger,
            dependency_triggers: self.dependency_triggers.clone(),
            data_requirements: self.data_requirements.clone(),
            trigger_requirements: self.trigger_requirements.clone(),
        })
    }

    /// Returns all the datasets present in the plan.
    pub fn datasets(&self) -> HashSet<&D> {
        self.datasets.iter().map(|(_, dataset)| dataset).collect()
    }

    /// Returns all the versions present in the plan.
    pub fn versions(&self) -> HashSet<(&D, &V)> {
        self.versions
            .iter()
            .map(|(_, (dataset_index, version))| {
                let dataset = self.datasets.get(dataset_index).unwrap();
                (dataset, version)
            })
            .collect()
    }

    /// Returns the manually triggered datasets in the execution plan.
    pub fn manual_trigger(&self) -> (&D, &V) {
        let (dataset_index, version) = self.versions.get(&self.manual_trigger).unwrap();
        let dataset = self.datasets.get(dataset_index).unwrap();
        (dataset, version)
    }

    /// Returns the dependency triggered datasets in the execution plan.
    pub fn dependency_triggers(&self) -> HashSet<(&D, &V)> {
        self.dependency_triggers
            .iter()
            .map(|version_index| {
                let (dataset_index, version) = self.versions.get(version_index).unwrap();
                let dataset = self.datasets.get(dataset_index).unwrap();
                (dataset, version)
            })
            .collect()
    }

    /// Returns all triggered datasets in the execution plan.
    pub fn triggers(&self) -> HashSet<&D> {
        self.dependency_triggers()
            .iter()
            .map(|(dataset, _)| *dataset)
            .chain(std::iter::once(self.manual_trigger().0))
            .collect()
    }

    /// Returns whether a dataset is a trigger in the execution plan.
    pub fn is_trigger(&self, dataset: &D) -> bool {
        let dataset_index = self.datasets.index(dataset).unwrap();
        let (manual_trigger_index, _) = self.versions.get(&self.manual_trigger).unwrap();
        dataset_index == manual_trigger_index
            || self.dependency_triggers.iter().any(|version_index| {
                let (dependency_index, _) = self.versions.get(version_index).unwrap();
                dependency_index == dataset_index
            })
    }

    /// Returns all requirements in the execution plan.
    pub fn data_requirements(&self) -> (Vec<Requirement<D, V>>, HashMap<&V, usize>) {
        self.normalize_requirements(&self.data_requirements)
    }

    /// Returns all requirements in the execution plan.
    pub fn trigger_requirements(&self) -> (Vec<Requirement<D, V>>, HashMap<&V, usize>) {
        self.normalize_requirements(&self.trigger_requirements)
    }

    /// Returns all requirements in the execution plan.
    pub fn requirements(&self) -> (Vec<Requirement<D, V>>, HashMap<&V, usize>) {
        let requirements = self
            .data_requirements
            .iter()
            .chain(&self.trigger_requirements)
            .cloned()
            .collect::<Vec<_>>();
        self.normalize_requirements(&requirements)
    }

    /// Helper function to get requirements from a given set of requirements, and the number of times
    /// the target versions are required.
    fn normalize_requirements(
        &self,
        requirements: &[(VersionIndex, VersionIndex)],
    ) -> (Vec<Requirement<D, V>>, HashMap<&V, usize>) {
        let mut version_count = HashMap::new();

        let normalized_requirements = requirements
            .iter()
            .map(|(target_index, source_index)| {
                let (target_index, target_version) = self.versions.get(target_index).unwrap();
                let target = self.datasets.get(target_index).unwrap();

                let (source_index, source_version) = self.versions.get(source_index).unwrap();
                let source = self.datasets.get(source_index).unwrap();

                // We only add existing versions to the count because versions that are never
                // to exist are not relevant for the count.
                *version_count.entry(target_version).or_insert(0) +=
                    source_version.existing_count();

                Requirement::new((target, target_version), (source, source_version))
            })
            .collect();

        (normalized_requirements, version_count)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Requirement<'a, D: DatasetRef, V: VersionRef>(((&'a D, &'a V), (&'a D, &'a V)));

impl<'a, D: DatasetRef, V: VersionRef> Requirement<'a, D, V> {
    pub fn new(target: (&'a D, &'a V), source: (&'a D, &'a V)) -> Requirement<'a, D, V> {
        Requirement((target, source))
    }

    pub fn target(&self) -> &D {
        self.0 .0 .0
    }

    pub fn target_version(&self) -> &V {
        self.0 .0 .1
    }

    pub fn source(&self) -> &D {
        self.0 .1 .0
    }

    pub fn source_version(&self) -> &V {
        self.0 .1 .1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::RelativeVersions::Plan;
    use crate::dataset::{Dataset, TdVersions};
    use std::convert::Infallible;
    use td_common::uri::Version::Head;
    use td_common::uri::Versions::Single;

    #[test]
    fn test_datasets() {
        let dataset = Dataset::new("dst", "d1");
        let template = ExecutionTemplate::with_trigger(&dataset);
        let datasets = template.datasets();
        assert!(datasets.contains(&dataset));
    }

    #[test]
    fn test_versions() {
        let dataset = Dataset::new("dst", "d1");
        let template = ExecutionTemplate::with_trigger(&dataset);
        let versions = template.versions();
        assert!(versions.contains(&(&dataset, &RelativeVersions::default())));
    }

    #[test]
    fn test_manual_trigger() {
        let dataset = Dataset::new("dst", "d1");
        let template = ExecutionTemplate::with_trigger(&dataset);
        let (manual_trigger_dataset, _) = template.manual_trigger();
        assert_eq!(manual_trigger_dataset, &dataset);
    }

    #[test]
    fn test_dependency_triggers() {
        let dataset = Dataset::new("dst", "d1");
        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_dependency_trigger(&dataset);
        let dependency_triggers = template.dependency_triggers();
        assert!(dependency_triggers.contains(&(&dataset, &RelativeVersions::default())));
    }

    #[test]
    fn test_triggers() {
        let dataset = Dataset::new("dst", "d1");
        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_dependency_trigger(&dataset);
        let triggers = template.triggers();
        assert!(triggers.contains(&dataset));
    }

    #[test]
    fn test_is_trigger() {
        let dataset = Dataset::new("dst", "d1");
        let template = ExecutionTemplate::with_trigger(&dataset);
        assert!(template.is_trigger(&dataset));
    }

    #[test]
    fn test_data_requirements() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");
        let version = Plan(TdVersions::from_table(
            Single(Head(-1)),
            "table".to_string(),
            0,
        ));

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_data_requirement(&dataset, &required_dataset, version.clone());

        let (data_requirements, _) = template.data_requirements();
        assert!(data_requirements.contains(&Requirement::new(
            (&dataset, &RelativeVersions::default()),
            (&required_dataset, &version),
        )));
    }

    #[test]
    fn test_trigger_requirements() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_trigger_requirement(&dataset, &required_dataset);

        let (trigger_requirements, _) = template.trigger_requirements();
        assert!(trigger_requirements.contains(&Requirement::new(
            (&dataset, &RelativeVersions::default()),
            (&required_dataset, &RelativeVersions::default()),
        )));
    }

    #[test]
    fn test_requirements() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");
        let version = Plan(TdVersions::from_table(
            Single(Head(-1)),
            "table".to_string(),
            0,
        ));

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_data_requirement(&dataset, &required_dataset, version.clone());
        template.add_trigger_requirement(&dataset, &required_dataset);

        let (requirements, _) = template.requirements();
        assert!(requirements.contains(&Requirement::new(
            (&dataset, &RelativeVersions::default()),
            (&required_dataset, &version),
        )));
        assert!(requirements.contains(&Requirement::new(
            (&dataset, &RelativeVersions::default()),
            (&required_dataset, &RelativeVersions::default()),
        )));
    }

    #[test]
    fn test_with_trigger() {
        let dataset = Dataset::new("dst", "d1");

        let template = ExecutionTemplate::with_trigger(&dataset);

        template.assert_datasets(&[&dataset]);
        template.assert_versions(&[(&dataset, &Plan(TdVersions::trigger()))]);
        template.assert_manual_trigger(&dataset);
        template.assert_dependency_triggers(&[]);
        template.assert_data_requirements(&[]);
        template.assert_trigger_requirements(&[]);
    }

    #[test]
    fn test_add_dependency_trigger() {
        let dataset = Dataset::new("dst", "d1");

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_dependency_trigger(&dataset);

        template.assert_datasets(&[&dataset]);
        template.assert_versions(&[(&dataset, &Plan(TdVersions::trigger()))]);
        template.assert_manual_trigger(&dataset);
        template.assert_dependency_triggers(&[&dataset]);
        template.assert_data_requirements(&[]);
        template.assert_trigger_requirements(&[]);
    }

    #[test]
    fn test_add_data_requirement() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");
        let version = Plan(TdVersions::from_table(
            Single(Head(-1)),
            "table".to_string(),
            0,
        ));

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_data_requirement(&dataset, &required_dataset, version.clone());

        template.assert_datasets(&[&dataset, &required_dataset]);
        template.assert_versions(&[
            (&required_dataset, &version),
            (&dataset, &Plan(TdVersions::trigger())),
        ]);
        template.assert_manual_trigger(&dataset);
        template.assert_dependency_triggers(&[]);
        template.assert_data_requirements(&[Requirement::new(
            (&dataset, &RelativeVersions::default()),
            (&required_dataset, &version),
        )]);
        template.assert_trigger_requirements(&[]);
    }

    #[test]
    fn test_add_trigger_requirement() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_trigger_requirement(&dataset, &required_dataset);

        template.assert_datasets(&[&dataset, &required_dataset]);
        template.assert_versions(&[
            (&required_dataset, &Plan(TdVersions::trigger())),
            (&dataset, &Plan(TdVersions::trigger())),
        ]);
        template.assert_manual_trigger(&dataset);
        template.assert_dependency_triggers(&[]);
        template.assert_data_requirements(&[]);
        template.assert_trigger_requirements(&[Requirement::new(
            (&dataset, &RelativeVersions::default()),
            (&required_dataset, &RelativeVersions::default()),
        )]);
    }

    #[tokio::test]
    async fn test_versioned() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");
        let version = Plan(TdVersions::from_table(
            Single(Head(-1)),
            "table".to_string(),
            0,
        ));

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_data_requirement(&dataset, &required_dataset, version.clone());

        let versioned = template
            .versioned(|_, v| async move { Ok::<_, Infallible>(format!("{:?}", v)) })
            .await
            .unwrap();

        versioned.assert_datasets(&[&dataset, &required_dataset]);
        versioned.assert_versions(&[
            (&required_dataset, &format!("{:?}", version)),
            (&dataset, &format!("{:?}", Plan(TdVersions::trigger()))),
        ]);
        versioned.assert_manual_trigger(&dataset);
        versioned.assert_dependency_triggers(&[]);
        versioned.assert_data_requirements(&[Requirement::new(
            (&dataset, &format!("{:?}", RelativeVersions::default())),
            (&required_dataset, &format!("{:?}", version)),
        )]);
        versioned.assert_trigger_requirements(&[]);
    }

    #[tokio::test]
    async fn test_versioned_with_different_transformations() {
        let dataset = Dataset::new("dst", "d1");
        let required_dataset = Dataset::new("dst", "d2");
        let version = Plan(TdVersions::from_table(
            Single(Head(-1)),
            "table".to_string(),
            0,
        ));

        let mut template = ExecutionTemplate::with_trigger(&dataset);
        template.add_data_requirement(&dataset, &required_dataset, version.clone());

        let versioned = template
            .versioned(|_, v| async move { Ok::<_, Infallible>(format!("{:?}", v)) })
            .await
            .unwrap();

        versioned.assert_datasets(&[&dataset, &required_dataset]);
        versioned.assert_versions(&[
            (&required_dataset, &format!("{:?}", version)),
            (&dataset, &format!("{:?}", Plan(TdVersions::trigger()))),
        ]);
        versioned.assert_manual_trigger(&dataset);
        versioned.assert_dependency_triggers(&[]);
        versioned.assert_data_requirements(&[Requirement::new(
            (&dataset, &format!("{:?}", RelativeVersions::default())),
            (&required_dataset, &format!("{:?}", version)),
        )]);
        versioned.assert_trigger_requirements(&[]);
    }
}
