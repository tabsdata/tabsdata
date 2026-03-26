//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::basic::{CollectionId, Private};
use sea_query::{Condition, Expr, ExprTrait, IntoTableRef};
use std::collections::HashSet;
use std::ops::Deref;
use ta_objects::errors::SqlError;
use ta_objects::{DaoCore, ListFilter, ListFilterExtract, SeaValue};
use td_error::TdError;
use td_tower::error::FromHandlerError;
use td_tower::extractors::{FromHandler, Ref};
use td_tower::handler::Handler;

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

/// Build a condition that restricts a column to a set of collection IDs.
fn collection_set_condition<D: DaoCore>(
    ids: &HashSet<CollectionId>,
) -> Result<Condition, SqlError> {
    let col = D::col_for_type(std::any::TypeId::of::<CollectionId>()).ok_or_else(|| {
        SqlError::TypeNotFound(
            std::any::type_name::<CollectionId>().to_string(),
            D::table().into_table_ref(),
        )
    })?;
    if ids.is_empty() {
        Ok(Condition::all().add(Expr::cust("1 = 0")))
    } else if ids.contains(&CollectionId::all_collections()) {
        Ok(Condition::all())
    } else {
        let values: Vec<_> = ids.iter().map(|id| id.as_sea_value()).collect();
        Ok(Condition::all().add(Expr::col(col).is_in(values)))
    }
}

impl ListFilter for VisibleCollections {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        // collection_id IN (direct UNION indirect)
        let all: HashSet<_> = self.direct().union(self.indirect()).cloned().collect();
        collection_set_condition::<D>(&all)
    }
}

impl ListFilterExtract for VisibleCollections {
    fn extract(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        let val = Ref::<Self>::from_handler(handler)?;
        Ok((*val).clone())
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![td_tower::metadata::type_of::<Self>()]
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

impl ListFilter for VisibleTablesCollections {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        let col = D::col_for_type(std::any::TypeId::of::<CollectionId>()).ok_or_else(|| {
            SqlError::TypeNotFound(
                std::any::type_name::<CollectionId>().to_string(),
                D::table().into_table_ref(),
            )
        })?;
        let private_col = D::col_for_type(std::any::TypeId::of::<Private>()).ok_or_else(|| {
            SqlError::TypeNotFound(
                std::any::type_name::<Private>().to_string(),
                D::table().into_table_ref(),
            )
        })?;

        // (collection_id IN direct) OR (collection_id IN indirect AND private = false)
        let direct = collection_set_condition::<D>(self.direct())?;
        let indirect_ids = self.indirect();
        let indirect = if indirect_ids.is_empty() {
            Condition::all().add(Expr::cust("1 = 0"))
        } else if indirect_ids.contains(&CollectionId::all_collections()) {
            Condition::all().add(Expr::col(private_col).eq(false))
        } else {
            let values: Vec<_> = indirect_ids.iter().map(|id| id.as_sea_value()).collect();
            Condition::all()
                .add(Expr::col(col).is_in(values))
                .add(Expr::col(private_col).eq(false))
        };
        Ok(Condition::any().add(direct).add(indirect))
    }
}

impl ListFilterExtract for VisibleTablesCollections {
    fn extract(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        let val = Ref::<Self>::from_handler(handler)?;
        Ok((*val).clone())
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![td_tower::metadata::type_of::<Self>()]
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

impl ListFilter for VisibleFunctionsCollections {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        // Only direct collections for functions
        collection_set_condition::<D>(self.direct())
    }
}

impl ListFilterExtract for VisibleFunctionsCollections {
    fn extract(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        let val = Ref::<Self>::from_handler(handler)?;
        Ok((*val).clone())
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![td_tower::metadata::type_of::<Self>()]
    }
}
