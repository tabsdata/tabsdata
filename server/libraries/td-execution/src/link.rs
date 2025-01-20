//
// Copyright 2025 Tabs Data Inc.
//

use derive_builder::Builder;
use getset::Getters;
use sqlx::FromRow;
use std::collections::HashSet;
use std::ops::Deref;

pub trait Link {
    fn source_collection_id(&self) -> &str;
    fn source_dataset_id(&self) -> &str;
    fn target_collection_id(&self) -> &str;
    fn target_dataset_id(&self) -> &str;
}

#[derive(Debug, Clone, Builder, FromRow, Getters)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct DataLink {
    source_collection_id: String,
    source_dataset_id: String,
    source_table: String,
    source_pos: i64,
    source_versions: String,
    target_collection_id: String,
    target_dataset_id: String,
}

impl Link for DataLink {
    fn source_collection_id(&self) -> &str {
        &self.source_collection_id
    }

    fn source_dataset_id(&self) -> &str {
        &self.source_dataset_id
    }

    fn target_collection_id(&self) -> &str {
        &self.target_collection_id
    }

    fn target_dataset_id(&self) -> &str {
        &self.target_dataset_id
    }
}

#[derive(Debug, Clone, Builder, FromRow, Getters)]
#[builder(setter(into))]
#[getset(get = "pub")]
pub struct TriggerLink {
    source_collection_id: String,
    source_dataset_id: String,
    target_collection_id: String,
    target_dataset_id: String,
}

impl Link for TriggerLink {
    fn source_collection_id(&self) -> &str {
        &self.source_collection_id
    }

    fn source_dataset_id(&self) -> &str {
        &self.source_dataset_id
    }

    fn target_collection_id(&self) -> &str {
        &self.target_collection_id
    }

    fn target_dataset_id(&self) -> &str {
        &self.target_dataset_id
    }
}

#[derive(Debug, Clone)]
pub struct Graph<L: Link>(pub Vec<L>);

impl<L: Link> Graph<L> {
    pub fn nodes(&self) -> HashSet<&str> {
        self.0
            .iter()
            .flat_map(|link| vec![link.source_dataset_id(), link.target_dataset_id()])
            .collect()
    }

    pub fn links(&self) -> &Vec<L> {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct DataGraph(pub Graph<DataLink>);

impl Deref for DataGraph {
    type Target = Graph<DataLink>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct TriggerGraph(pub Graph<TriggerLink>);

impl Deref for TriggerGraph {
    type Target = Graph<TriggerLink>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
