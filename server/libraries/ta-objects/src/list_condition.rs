//
// Copyright 2026 Tabs Data Inc.
//

use crate::dxo::DaoCore;
use crate::errors::SqlError;
use sea_query::Condition;
use td_tower::error::FromHandlerError;
use td_tower::extractors::FromHandler;
use td_tower::handler::Handler;

/// Trait for applying visibility / authorization filters to list queries.
///
/// Implementors produce a `sea_query::Condition` that is ANDed into the
/// base query. The condition is resolved against the DAO's column map
/// via `DaoCore::col_for_type`.
pub trait ListFilter: Send + Sync {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError>;
}

/// No-op filter — matches all rows.
#[derive(Clone)]
pub struct NoFilter;

impl ListFilter for NoFilter {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        Ok(Condition::all())
    }
}

/// Extracts a [`ListFilter`] from a tower [`Handler`].
pub trait ListFilterExtract: ListFilter + Clone + Send + 'static {
    fn extract(handler: &mut Handler) -> Result<Self, FromHandlerError>;

    /// Returns the handler types actually consumed by this extractor.
    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String>;
}

impl ListFilterExtract for NoFilter {
    fn extract(_handler: &mut Handler) -> Result<Self, FromHandlerError> {
        Ok(NoFilter)
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        vec![]
    }
}

/// Extracts a [`ListFilterExtract`] from the tower [`Handler`].
///
/// Follows the same pattern as [`SqlRef`](crate::sql_condition::SqlRef):
/// implements [`Deref`](std::ops::Deref) so the inner filter is accessed
/// without destructuring.
#[derive(Debug, Clone)]
pub struct ListRef<F>(F);

impl<F> ListRef<F> {
    pub fn new(value: F) -> Self {
        Self(value)
    }
}

impl<F> std::ops::Deref for ListRef<F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: ListFilter> ListFilter for ListRef<F> {
    fn condition<D: DaoCore>(&self) -> Result<Condition, SqlError> {
        self.0.condition::<D>()
    }
}

impl<F: ListFilterExtract> FromHandler for ListRef<F> {
    fn from_handler(handler: &mut Handler) -> Result<Self, FromHandlerError> {
        Ok(ListRef(F::extract(handler)?))
    }

    #[cfg(feature = "test_tower_metadata")]
    fn metadata_used_types() -> Vec<String> {
        F::metadata_used_types()
    }
}
