//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::raw_sql::seaql::Query;
use std::ops::Deref;

#[derive(Debug)]
pub struct RawSeaQuery(pub Query);

impl Deref for RawSeaQuery {
    type Target = Query;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Query> for RawSeaQuery {
    fn from(query: Query) -> Self {
        Self(query)
    }
}
