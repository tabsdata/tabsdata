//
// Copyright 2025 Tabs Data Inc.
//

use crate::sql::ListFilterGenerator;
use crate::types::DataAccessObject;
use crate::types::basic::{CollectionId, Private};
use async_trait::async_trait;
use sqlx::{QueryBuilder, Sqlite};
use std::collections::HashSet;
use std::ops::Deref;
use td_error::TdError;

#[derive(Debug, Clone)]
pub struct VisibleCollections(HashSet<CollectionId>, HashSet<CollectionId>);

impl VisibleCollections {
    pub fn new(visible: HashSet<CollectionId>, indirect: HashSet<CollectionId>) -> Self {
        Self(visible, indirect)
    }

    pub fn direct(&self) -> &HashSet<CollectionId> {
        &self.0
    }

    pub fn indirect(&self) -> &HashSet<CollectionId> {
        &self.1
    }
}

fn collections_where<'a>(
    query_builder: &mut QueryBuilder<'a, Sqlite>,
    field: &str,
    collections: &'a HashSet<CollectionId>,
) -> Result<(), TdError> {
    query_builder.push("(");
    if collections.is_empty() {
        query_builder.push("1 = 0"); // if no collections, we need to ensure the condition is false
    } else if collections.contains(&CollectionId::all_collections()) {
        query_builder.push("1 = 1"); // if all collections, we need to ensure the condition is true
    } else {
        query_builder.push(format!("{field} IN ("));
        let mut separated = query_builder.separated(", ");
        for collection_id in collections {
            separated.push_bind(collection_id);
        }
        query_builder.push(")");
    }
    query_builder.push(")");
    Ok(())
}

#[async_trait]
impl ListFilterGenerator for VisibleCollections {
    fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        with_where: bool,
        query_builder: &mut QueryBuilder<'a, Sqlite>,
    ) -> Result<bool, TdError> {
        let mut with_where = with_where;
        if with_where {
            query_builder.push(" AND ");
        } else {
            query_builder.push(" WHERE ");
            with_where = true;
        }

        let field = D::sql_field_for_type(std::any::TypeId::of::<CollectionId>())?;
        query_builder.push("(");
        collections_where(query_builder, field, self.direct())?;
        query_builder.push(" OR ");
        collections_where(query_builder, field, self.indirect())?;
        query_builder.push(")");

        Ok(with_where)
    }
}

#[derive(Debug, Clone)]
pub struct VisibleTablesCollections(VisibleCollections);

impl Deref for VisibleTablesCollections {
    type Target = VisibleCollections;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&VisibleCollections> for VisibleTablesCollections {
    type Error = TdError;

    fn try_from(visible: &VisibleCollections) -> Result<Self, TdError> {
        Ok(Self(visible.clone()))
    }
}

// TODO this is only allows to tables, that have CollectionId and Private. We should make this
// more generic and resilient.
impl ListFilterGenerator for VisibleTablesCollections {
    fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        with_where: bool,
        query_builder: &mut QueryBuilder<'a, Sqlite>,
    ) -> Result<bool, TdError> {
        let mut with_where = with_where;
        if with_where {
            query_builder.push(" AND ");
        } else {
            query_builder.push(" WHERE ");
            with_where = true;
        }

        let field = D::sql_field_for_type(std::any::TypeId::of::<CollectionId>())?;
        query_builder.push("(");
        collections_where(query_builder, field, self.direct())?;
        query_builder.push(" OR ");

        let private = D::sql_field_for_type(std::any::TypeId::of::<Private>())?;
        query_builder.push("(");
        if self.indirect().is_empty() {
            query_builder.push("1 = 0"); // if no collections, we need to ensure the condition is false
        } else if self.indirect().contains(&CollectionId::all_collections()) {
            query_builder.push("1 = 1"); // if all collections, we need to ensure the condition is true
        } else {
            query_builder.push(format!("{field} IN ("));
            let mut separated = query_builder.separated(", ");
            for collection_id in self.indirect() {
                separated.push_bind(collection_id);
                separated.push_bind(format!(" AND {private} = false")); // only non-private tables
            }
            query_builder.push(")");
        }
        query_builder.push(")");

        query_builder.push(")");

        Ok(with_where)
    }
}

#[derive(Debug, Clone)]
pub struct VisibleFunctionsCollections(VisibleCollections);

impl Deref for VisibleFunctionsCollections {
    type Target = VisibleCollections;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&VisibleCollections> for VisibleFunctionsCollections {
    type Error = TdError;

    fn try_from(visible: &VisibleCollections) -> Result<Self, TdError> {
        Ok(Self(visible.clone()))
    }
}

// TODO this is only allows functions of direct collections. We should make this
// more generic and resilient.
impl ListFilterGenerator for VisibleFunctionsCollections {
    fn where_clause<'a, D: DataAccessObject>(
        &'a self,
        with_where: bool,
        query_builder: &mut QueryBuilder<'a, Sqlite>,
    ) -> Result<bool, TdError> {
        let mut with_where = with_where;
        if with_where {
            query_builder.push(" AND ");
        } else {
            query_builder.push(" WHERE ");
            with_where = true;
        }

        let field = D::sql_field_for_type(std::any::TypeId::of::<CollectionId>())?;
        query_builder.push("(");
        collections_where(query_builder, field, self.direct())?;
        query_builder.push(")");

        Ok(with_where)
    }
}
