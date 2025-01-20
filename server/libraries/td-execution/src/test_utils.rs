//
// Copyright 2025 Tabs Data Inc.
//

use crate::dataset::Dataset;
use crate::execution_planner::{ExecutionPlanner, Requirement};
use crate::graphs::DatasetGraph;
use crate::link::{DataLink, DataLinkBuilder, TriggerLink, TriggerLinkBuilder};
use petgraph::prelude::NodeIndex;
use td_common::dataset::{DatasetRef, VersionRef};

/// Node finder trait used in tests, to assets that it is what we expect.
pub trait NodeFinder<D: DatasetRef> {
    fn node_index_for_dataset(&self, dataset: &D) -> Option<NodeIndex>;
}

impl<D: DatasetRef, V: VersionRef> NodeFinder<D> for DatasetGraph<D, V> {
    fn node_index_for_dataset(&self, dataset: &D) -> Option<NodeIndex> {
        self.graph().node_indices().find(|i| {
            let node = &self.graph()[*i];
            node == dataset
        })
    }
}

/// Create a data link from a source dataset to a target dataset.
pub fn data_link_from_dataset(source: &Dataset, target: &Dataset, versions: &str) -> DataLink {
    DataLinkBuilder::default()
        .source_collection_id(source.collection().to_string())
        .source_dataset_id(source.dataset().to_string())
        .source_table("table".to_string())
        .source_pos(0)
        .source_versions(versions.to_string())
        .target_collection_id(target.collection().to_string())
        .target_dataset_id(target.dataset().to_string())
        .build()
        .unwrap()
}

/// Create a trigger link from a source dataset to a target dataset.
pub fn trigger_link_from_dataset(source: &Dataset, target: &Dataset) -> TriggerLink {
    TriggerLinkBuilder::default()
        .source_collection_id(source.collection().to_string())
        .source_dataset_id(source.dataset().to_string())
        .target_collection_id(target.collection().to_string())
        .target_dataset_id(target.dataset().to_string())
        .build()
        .unwrap()
}

/// Trait to assert that the execution planner is what we expect.
impl<D: DatasetRef, V: VersionRef> ExecutionPlanner<D, V> {
    pub fn assert_datasets(&self, e_datasets: &[&D]) {
        assert_eq!(self.datasets().len(), e_datasets.len());
        let datasets = self.datasets();
        for dataset in e_datasets {
            assert!(datasets.contains(*dataset));
        }
    }

    pub fn assert_versions(&self, e_versions: &[(&D, &V)]) {
        assert_eq!(self.versions().len(), e_versions.len());
        let versions = self.versions();
        for version in e_versions {
            assert!(versions.contains(version));
        }
    }

    pub fn assert_manual_trigger(&self, e_manual_trigger: &D) {
        let versions = self.versions();
        let (dataset_index, _) = versions.get(&self.manual_trigger()).unwrap();
        let datasets = self.datasets();
        let dataset = datasets.get(dataset_index).unwrap();
        assert_eq!(dataset, &e_manual_trigger);
    }

    pub fn assert_dependency_triggers(&self, e_dependency_triggers: &[&D]) {
        assert_eq!(
            self.dependency_triggers().len(),
            e_dependency_triggers.len()
        );
        let triggers: Vec<_> = self
            .dependency_triggers()
            .iter()
            .map(|version_index| {
                let versions = self.versions();
                let (dataset_index, _) = versions.get(version_index).unwrap();
                let datasets = self.datasets();
                *datasets.get(dataset_index).unwrap()
            })
            .collect();
        for dataset in e_dependency_triggers {
            assert!(triggers.contains(dataset));
        }
    }

    pub fn assert_data_requirements(&self, e_requirements: &[Requirement<D, V>]) {
        assert_eq!(self.data_requirements().0.len(), e_requirements.len());
        let (requirements, _) = self.data_requirements();
        for requirement in e_requirements {
            assert!(requirements.contains(requirement));
        }
    }

    pub fn assert_trigger_requirements(&self, e_requirements: &[Requirement<D, V>]) {
        assert_eq!(self.trigger_requirements().0.len(), e_requirements.len());
        let (requirements, _) = self.trigger_requirements();
        for requirement in e_requirements {
            assert!(requirements.contains(requirement));
        }
    }
}
